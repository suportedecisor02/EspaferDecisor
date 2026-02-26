import bcrypt
import streamlit as st
import pandas as pd
import psycopg2
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from fpdf import FPDF
import io
import datetime
import logging
from logging.handlers import RotatingFileHandler
import re
from typing import List

# ==================== CONFIGURAÇÃO DE LOGGING COM ROTAÇÃO ====================
# Máximo de 5MB por arquivo, mantém 3 backups — evita crescimento ilimitado
_log_handler_file = RotatingFileHandler('app.log', maxBytes=5*1024*1024, backupCount=3, encoding='utf-8')
_log_handler_file.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
_log_handler_stream = logging.StreamHandler()
_log_handler_stream.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

logging.basicConfig(level=logging.INFO, handlers=[_log_handler_file, _log_handler_stream])
logger = logging.getLogger(__name__)

# ==================== UTILITÁRIO: mascarar e-mail para log ====================
def _mascarar_email(email: str) -> str:
    """Retorna e-mail parcialmente mascarado para uso em logs (ex: na***@dominio.com)."""
    try:
        usuario, dominio = email.split('@', 1)
        visivel = usuario[:2] if len(usuario) > 2 else usuario[0]
        return f"{visivel}***@{dominio}"
    except Exception:
        return "***"

# 1. Configuração de Página
st.set_page_config(page_title="Rede Espafer", layout="wide")

# ==================== CONSTANTES ====================
BLACKLIST_FILIAIS = set()

# ==================== FUNÇÕES AUXILIARES ====================
def validar_periodo_datas(data_inicial, data_final, max_dias=365):
    """Valida período entre datas."""
    if data_inicial > data_final:
        raise ValueError("Data inicial não pode ser maior que data final")
    
    diferenca = (data_final - data_inicial).days
    if diferenca > max_dias:
        raise ValueError(f"Período não pode exceder {max_dias} dias")
    
    return True


# --- CLASSE PARA GERAR O PDF ---
class PDF(FPDF):
    # Adicionado parâmetro 'info_extra'
    def __init__(self, titulo="SOLICITAÇÃO DE COTAÇÃO", subtitulo="Departamento de Compras", fornecedor=None, info_extra=None):
        super().__init__()
        self.titulo_rel = titulo
        self.subtitulo_rel = subtitulo
        self.fornecedor = fornecedor
        self.info_extra = info_extra
        self.data_emissao = datetime.date.today().strftime("%d/%m/%Y")

    def header(self):
        # Linha superior vermelha (Marca)
        self.set_draw_color(179, 0, 0)
        self.set_line_width(1)
        self.line(10, 10, 200, 10)
        
        # Título e Subtítulo
        self.ln(5)
        self.set_font('Arial', 'B', 18)
        self.set_text_color(50, 50, 50)
        self.cell(0, 10, 'REDE ESPAFER', 0, 1, 'L')
        
        subtitulo_limpo = self.subtitulo_rel.encode('latin-1', 'replace').decode('latin-1')
        self.set_font('Arial', '', 10)
        self.set_text_color(100, 100, 100)
        self.cell(0, 5, subtitulo_limpo, 0, 1, 'L')
        
        # Info do Relatório / Fornecedor (Box Cinza)
        self.ln(5)
        self.set_fill_color(245, 245, 245)
        self.rect(10, 35, 190, 20, 'F')
        
        self.set_y(38)
        self.set_font('Arial', 'B', 10)
        self.set_text_color(0, 0, 0)
        self.cell(15, 5, '', 0, 0)
        
        fornecedor_txt = self.fornecedor if self.fornecedor else ""
        titulo_txt = self.titulo_rel if self.titulo_rel else ""
        
        texto_principal = f'FORNECEDOR: {fornecedor_txt}' if self.fornecedor else f'RELATÓRIO: {titulo_txt}'
        texto_principal = texto_principal.encode('latin-1', 'replace').decode('latin-1')

        self.cell(95, 5, texto_principal, 0, 0)
        self.cell(80, 5, f'DATA EMISSÃO: {self.data_emissao}', 0, 1, 'R')
        
        self.set_font('Arial', '', 9)
        self.cell(15, 5, '', 0, 0)
        
        if self.fornecedor:
            texto_secundario = 'Favor conferir itens e quantidades.'
        elif self.info_extra:
            texto_secundario = self.info_extra 
        else:
            texto_secundario = 'Documento para uso interno de gestão de estoque.'
            
        texto_secundario = texto_secundario.encode('latin-1', 'replace').decode('latin-1')
        self.cell(175, 5, texto_secundario, 0, 1)
        self.ln(10)

    def footer(self):
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        self.set_text_color(150)
        self.cell(0, 10, f'Rede Espafer - Página {self.page_no()}', 0, 0, 'C')

class DatabaseManager:
    """Gerencia conexões e queries ao banco PostgreSQL."""
    
    def __init__(self):
        if "postgres" not in st.secrets:
            logger.error("Seção [postgres] não encontrada no secrets.toml")
            st.error("Erro: Seção [postgres] não encontrada no secrets.toml")
            self.creds = None
        else:
            self.creds = st.secrets["postgres"]
            logger.info("DatabaseManager inicializado com sucesso")

    def get_blacklist(self) -> set:
        """Retorna conjunto de filiais bloqueadas."""
        return BLACKLIST_FILIAIS

    def _get_connection(self):
        """Cria conexão com o banco de dados."""
        #password = urllib.parse.quote_plus(self.creds.get("password"))
        try:
            st.error(self.creds.get("host"))
            st.error(self.creds.get("database"))
            st.error(self.creds.get("username"))
            st.error(self.creds.get("password"))
            st.error(self.creds.get("port"))
            return psycopg2.connect(
                host=self.creds.get("host"),
                database=self.creds.get("database"),
                user=self.creds.get("user") or self.creds.get("username"),
                password=self.creds.get("password"),
                port=int(self.creds.get("port", 5432))
            )
        except psycopg2.OperationalError as e:
            logger.error(f"Erro de conexão com banco de dados: {e}")
            raise
        except Exception as e:
            logger.error(f"Erro inesperado ao conectar: {e}")
            raise

    def validar_usuario(self, usuario, senha):
        """
        Valida o usuário com compatibilidade dupla:
        - Se a senha no banco começa com '$2b$' ou '$2a$', trata como hash bcrypt.
        - Caso contrário, compara em texto puro (senhas antigas ainda não migradas)
          e aproveita para fazer a migração automatica para bcrypt na mesma operacao.
        """
        usuario_limpo = str(usuario).strip()
        senha_digitada = str(senha).strip()
        senha_bytes = senha_digitada.encode('utf-8')

        query = "SELECT usuario, nome, senha FROM usuarios_sistema WHERE LOWER(usuario) = LOWER(%s) AND ativo = TRUE"

        conn = None
        try:
            conn = self._get_connection()
            if not conn:
                return None
            cursor = conn.cursor()
            
            cursor.execute(query, (usuario_limpo,))
            res = cursor.fetchone()

            if not res:
                cursor.close()
                return None

            usuario_bd, nome_bd, senha_bd = res
            senha_bd = str(senha_bd).strip()
            autenticado = False

            if senha_bd.startswith('$2b$') or senha_bd.startswith('$2a$'):
                try:
                    autenticado = bcrypt.checkpw(senha_bytes, senha_bd.encode('utf-8'))
                except Exception:
                    autenticado = False
            else:
                # --- Senha ainda em texto puro: compara diretamente ---
                autenticado = (senha_digitada == senha_bd)

                if autenticado:
                    # Migracao automatica: salva o hash bcrypt no banco
                    try:
                        novo_hash = bcrypt.hashpw(senha_bytes, bcrypt.gensalt()).decode('utf-8')
                        cursor.execute(
                            "UPDATE usuarios_sistema SET senha = %s WHERE LOWER(usuario) = LOWER(%s)",
                            (novo_hash, usuario_limpo)
                        )
                        conn.commit()
                        logger.info(f"Senha do usuario migrada para bcrypt com sucesso.")
                    except Exception as e:
                        logger.warning(f"Nao foi possivel migrar a senha para bcrypt: {e}")

            cursor.close()
            return (usuario_bd, nome_bd) if autenticado else None

        except Exception as e:
            logger.error(f"Erro ao acessar tabela de usuarios: {e}")
            st.error(f"Erro ao verificar credenciais. Tente novamente. {e}")
            return None
        finally:
            if conn:
                conn.close()

    @st.cache_data(ttl=3600, show_spinner=False)
    def _ordenar_naturalmente(self, lista):
        """Função auxiliar para ordenar listas misturando números e textos corretamente."""
        converter = lambda texto: int(texto) if texto.isdigit() else texto.lower()
        chave_alfanumerica = lambda chave: [converter(c) for c in re.split(r'(\d+)', str(chave))]
        return sorted(lista, key=chave_alfanumerica)
    
    def buscar_filiais(self) -> List[str]:
        if not self.creds: return []
        try:
            conn = self._get_connection()
            query = "SELECT DISTINCT nome_empresa FROM venda_itens_consolidado WHERE nome_empresa IS NOT NULL ORDER BY 1"
            df = pd.read_sql(query, conn)
            conn.close()
            return df['nome_empresa'].tolist()
        except Exception as e:
            logger.error(f"Erro ao buscar filiais: {e}")
            return []
        
    def buscar_marcas(self, filial=None):
        if not self.creds: return []
        try:
            conn = self._get_connection()
            query = "SELECT DISTINCT marca FROM cad_produto WHERE marca IS NOT NULL AND marca != ''"
            query += " ORDER BY 1"
            df = pd.read_sql(query, conn)
            conn.close()
            return df.iloc[:, 0].tolist() if not df.empty else []
        except: return []

    def buscar_grupos(self, filial=None, marca=None):
        if not self.creds: return []
        try:
            conn = self._get_connection()
            query = "SELECT DISTINCT nome_grupo FROM cad_produto WHERE nome_grupo IS NOT NULL AND nome_grupo != ''"
            params = []
            if marca and marca != "TODOS":
                query += " AND marca = %s"; params.append(marca)
            query += " ORDER BY 1"
            df = pd.read_sql(query, conn, params=params)
            conn.close()
            return df.iloc[:, 0].tolist() if not df.empty else []
        except: return []

    def buscar_subgrupos(self, filial=None, marca=None, grupo=None):
        if not self.creds: return []
        try:
            conn = self._get_connection()
            query = "SELECT DISTINCT nome_sub_grupo FROM cad_produto WHERE nome_sub_grupo IS NOT NULL"
            params = []
            if marca and marca != "TODOS":
                query += " AND marca = %s"; params.append(marca)
            if grupo and grupo != "TODOS":
                query += " AND nome_grupo = %s"; params.append(grupo)
            query += " ORDER BY 1"
            df = pd.read_sql(query, conn, params=params)
            conn.close()
            return df.iloc[:, 0].tolist() if not df.empty else []
        except: return []

    def buscar_subgrupos1(self, filial=None, marca=None, grupo=None, subgrupo=None):
        if not self.creds: return []
        try:
            conn = self._get_connection()
            # Buscamos na coluna correta: nome_sub_grupo1
            query = "SELECT DISTINCT nome_sub_grupo1 FROM cad_produto WHERE nome_sub_grupo1 IS NOT NULL AND nome_sub_grupo1 != ''"
            params = []
            
            if marca and marca != "TODOS":
                query += " AND marca = %s"; params.append(marca)
            if grupo and grupo != "TODOS":
                query += " AND nome_grupo = %s"; params.append(grupo)
            if subgrupo and subgrupo != "TODOS":
                query += " AND nome_sub_grupo = %s"; params.append(subgrupo)
                
            query += " ORDER BY 1"
            df = pd.read_sql(query, conn, params=params)
            conn.close()
            return df.iloc[:, 0].tolist() if not df.empty else []
        except Exception as e:
            logger.error(f"Erro ao buscar subgrupo1: {e}")
            return []

    def buscar_produtos(self, filial=None, marca=None, grupo=None, subgrupo=None, subgrupo1=None):
        if not self.creds: return []
        try:
            conn = self._get_connection()
            query = "SELECT DISTINCT CONCAT(TRIM(CAST(codacessog AS TEXT)), ' - ', TRIM(nome)) FROM cad_produto WHERE codacessog IS NOT NULL"
            params = []
            if marca and marca != "TODOS":
                query += " AND marca = %s"; params.append(marca)
            if grupo and grupo != "TODOS":
                query += " AND nome_grupo = %s"; params.append(grupo)
            if subgrupo and subgrupo != "TODOS":
                query += " AND nome_sub_grupo = %s"; params.append(subgrupo)
            if subgrupo1 and subgrupo1 != "TODOS":
                query += " AND nome_sub_grupo1 = %s"; params.append(subgrupo1)
            query += " ORDER BY 1"
            df = pd.read_sql(query, conn, params=params)
            conn.close()
            return df.iloc[:, 0].tolist() if not df.empty else []
        except: return []

    def buscar_fornecedores(self):
        """Busca a lista de fornecedores cadastrados."""
        if not self.creds: return pd.DataFrame()
        conn = None
        try:
            conn = self._get_connection()
            query = "SELECT fornecedor, email, marca FROM fornecedores ORDER BY fornecedor"
            df = pd.read_sql(query, conn)
            df.columns = [c.lower() for c in df.columns]
            return df
        except Exception as e:
            logger.error(f"Erro ao buscar fornecedores: {e}")
            return pd.DataFrame(columns=['fornecedor', 'email', 'marca'])
        finally:
            if conn: conn.close()

    def consultar_cobertura(self, filial, marca, grupo, subgrupo, subgrupo1, produto, dias_alvo, dias_corte, modo):
        if not self.creds: return pd.DataFrame()
        conn = None
        try:
            conn = self._get_connection()
            # Garante valores numéricos limpos — sem interpolação de strings
            d_alv = int(float(dias_alvo)) if dias_alvo > 0 else 35
            d_cor = float(dias_corte) if dias_corte >= 0 else 0.0

            # Configurações de exibição
            op = '>' if modo == 'SOBRA' else '<'
            ordem = "dias_estoque ASC" if modo == 'COMPRA' else "dias_estoque DESC"

            if modo == 'COMPRA':
                v_ex = " AND venda_periodo > 0 AND reposicao > 0 AND vol_estoque >= 0"
                colunas_selecionadas = "filial, idproduto, produto, marca, fornecedor, grupo, subgrupo, subgrupo1, vol_estoque, venda_periodo, dias_estoque, reposicao"
            else:
                v_ex = " AND vol_estoque > 0 AND dias_estoque > 0"
                colunas_selecionadas = "filial, idproduto, produto, marca, grupo, subgrupo, subgrupo1, vol_estoque, venda_periodo, dias_estoque"

            rep_sql = "GREATEST(CEIL(venda_periodo - GREATEST(vol_estoque, 0)), 0)" if modo == 'COMPRA' else "0"

            # ----------------------------------------------------------------
            # SEGURANÇA: filtro de filial via parâmetro, nunca f-string
            # A lógica SPLIT_PART precisa ser montada de forma segura:
            # - quando há filial específica, passamos o valor como parâmetro %s
            # - quando é "TODOS", usamos a coluna da tabela
            # ----------------------------------------------------------------
            params_ven = [d_alv]  # Primeiro parâmetro: intervalo de dias
            if filial and filial not in ["TODAS", "TODOS"]:
                f_fil_venda = " AND v.nome_empresa = %s"
                col_fil_sql = "SPLIT_PART(SPLIT_PART(%s, '[', 2), ']', 1)"
                params_ven.append(filial)       # para o WHERE
                params_col = [filial]           # para o SPLIT_PART na SELECT
            else:
                f_fil_venda = ""
                col_fil_sql = "SPLIT_PART(SPLIT_PART(v.n_fil, '[', 2), ']', 1)"
                params_col = []

            # Monta a query base — apenas literais SQL, sem dados do usuário interpolados
            q = f"""
            WITH est AS (
                SELECT TRIM(CAST(codigo AS TEXT)) as cod,
                       SUM(COALESCE(NULLIF(qtde, '')::numeric, 0)) as total
                FROM arq_prod_estoque
                WHERE codlocal IN (5, 7)
                GROUP BY 1
            ),
            ven AS (
                SELECT
                    TRIM(CAST(v.codacessog AS TEXT)) as cod,
                    MAX(v.nome_empresa) as n_fil,
                    SUM(v.qtde::numeric) as per
                FROM venda_itens_consolidado v
                WHERE v.data >= CURRENT_DATE - INTERVAL '%s days'
                {f_fil_venda}
                GROUP BY 1
            ),
            base AS (
                SELECT
                    {col_fil_sql} as filial,
                    TRIM(CAST(c.codacessog AS TEXT)) as "idproduto",
                    c.nome as produto,
                    c.marca,
                    f.fornecedor,
                    c.nome_grupo as grupo,
                    c.nome_sub_grupo as subgrupo,
                    c.nome_sub_grupo1 as subgrupo1,
                    ROUND(COALESCE(e.total, 0), 2) as vol_estoque,
                    ROUND(COALESCE(v.per, 0), 2) as venda_periodo,
                    CASE
                        WHEN COALESCE(e.total, 0) <= 0 THEN 0
                        WHEN COALESCE(v.per, 0) > 0 THEN ROUND((COALESCE(e.total, 0) / (v.per / %s))::numeric, 1)
                        WHEN COALESCE(e.total, 0) > 0 THEN 999
                        ELSE 0
                    END as dias_estoque
                FROM cad_produto c
                LEFT JOIN est e ON TRIM(CAST(c.codigo AS TEXT)) = e.cod
                LEFT JOIN ven v ON TRIM(CAST(c.codacessog AS TEXT)) = v.cod
                LEFT JOIN fornecedores f ON TRIM(UPPER(c.marca)) = TRIM(UPPER(f.marca))
            )
            SELECT {colunas_selecionadas} FROM (
                SELECT *, {rep_sql} as reposicao FROM base
            ) sub
            WHERE 1=1
            """

            # Parâmetros posicionais para a query base
            # Ordem: d_alv (INTERVAL), [filial para WHERE], d_alv (divisão), [filial para SPLIT_PART]
            params_base = params_col + params_ven + [d_alv]

            # ----------------------------------------------------------------
            # SEGURANÇA: filtros dinâmicos todos via parâmetro %s
            # ----------------------------------------------------------------
            filtros_dinamicos = []
            params_filtros = []

            if marca and marca not in ["TODAS", "TODOS"]:
                filtros_dinamicos.append(" AND marca = %s")
                params_filtros.append(marca)

            if grupo and grupo not in ["TODAS", "TODOS"]:
                filtros_dinamicos.append(" AND grupo = %s")
                params_filtros.append(grupo)

            if subgrupo and subgrupo not in ["TODAS", "TODOS"]:
                filtros_dinamicos.append(" AND subgrupo = %s")
                params_filtros.append(subgrupo)

            if subgrupo1 and subgrupo1 not in ["TODAS", "TODOS"]:
                filtros_dinamicos.append(" AND subgrupo1 = %s")
                params_filtros.append(subgrupo1)

            if produto and produto not in ["TODAS", "TODOS"]:
                p_id = str(produto).split(' - ')[0].strip()
                filtros_dinamicos.append(" AND idproduto = %s")
                params_filtros.append(p_id)

            q += "".join(filtros_dinamicos)
            # op e d_cor são controlados internamente (não vêm do usuário diretamente como texto livre)
            q += f" AND dias_estoque {op} %s {v_ex} ORDER BY {ordem} LIMIT 2000"
            params_filtros.append(d_cor)

            todos_params = tuple(params_base + params_filtros)

            df = pd.read_sql(q, conn, params=todos_params)
            df.columns = [c.lower() for c in df.columns]

            if len(df) == 2000:
                logger.warning("Consulta retornou o limite de 2000 registros — resultado pode estar truncado.")

            return df

        except Exception as e:
            logger.error(f"Erro na consulta de cobertura: {e}")
            return pd.DataFrame()
        finally:
            if conn:
                conn.close()
class AppClientePrime:
    def __init__(self):
        self.db = DatabaseManager()
        self.inicializar_estado()
        self.aplicar_estilos()
        # A lista fixa self.emails_fornecedores foi removida daqui
        # Agora é gerenciada pelo st.session_state em inicializar_estado

    def inicializar_estado(self):
        if 'menu_ativo' not in st.session_state: st.session_state.menu_ativo = "Gerar Cobertura"
        if 'dados_orcamento' not in st.session_state: st.session_state.dados_orcamento = None
        if 'modo_analise_atual' not in st.session_state: st.session_state.modo_analise_atual = "COMPRA"
        if 'tentativas_login' not in st.session_state: st.session_state.tentativas_login = 0

        # Fornecedores são carregados do banco em tela_orcamento().
        # Inicializa como dict vazio — sem e-mails hardcoded no código.
        if 'db_fornecedores' not in st.session_state:
            st.session_state.db_fornecedores = {}

    def aplicar_estilos(self):
        
        if st.session_state.menu_ativo == "Inteligência de Compra":
            hover_secondary = """
                section[data-testid="stMain"] div.stButton > button[kind="secondary"]:hover {
                    background-color: #262730 !important; 
                    color: #0047AB !important;
                    border-color: #0047AB !important;
                    box-shadow: 0 4px 6px rgba(0,0,0,0.3) !important;
                }
            """
        else:
            hover_secondary = """
                section[data-testid="stMain"] div.stButton > button[kind="secondary"]:hover {
                    background-color: #F0F2F6 !important;
                    color: #31333F !important;
                    border-color: #CCCCCC !important;
                }
            """
            
        st.markdown("""
            <style>
                @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;700;900&display=swap');
                
                /* Configurações Gerais */
                .stApp { background-color: #FFFFFF !important; }
                
                /* --- SIDEBAR --- */
                section[data-testid="stSidebar"] { background-color: #121212 !important; border-right: 2px solid #0047AB !important; }
                section[data-testid="stSidebar"] .stButton > button { background-color: transparent !important; color: #FFFFFF !important; border: none !important; width: 100%; text-align: left; }
                section[data-testid="stSidebar"] div:has(button[kind="primary"]) > div > button { background-color: #FFFFFF !important; color: #000000 !important; font-weight: 900 !important; }
                
                /* --- EXPANDER --- */
                div[data-testid="stExpander"] { background-color: transparent !important; border: none !important; box-shadow: none !important; }
                div[data-testid="stExpander"] details { border: none !important; }
                div[data-testid="stExpander"] > details > summary {
                    background: linear-gradient(135deg, #0047AB 0%, #000000 150%) !important;
                    color: white !important; font-weight: 900 !important; border-radius: 8px !important;
                    padding: 0.75rem 1rem !important; margin-bottom: 5px !important; transition: opacity 0.2s ease-in-out !important;
                }
                div[data-testid="stExpander"] > details > summary:hover { opacity: 0.9 !important; color: #FFFFFF !important; }
                
                /* --- TEXTOS E INPUTS --- */
                div[data-testid="stSpinner"] p { color: #000000 !important; font-weight: 700 !important; }
                [data-testid="stWidgetLabel"] p { color: #000000 !important; font-weight: 700 !important; font-size: 14px !important; }
                .stTextInput input, .stNumberInput input, .stSelectbox div[data-baseweb="select"] > div { 
                    background-color: #F8F9FA !important; border: 1px solid #DEE2E6 !important; 
                    color: #000000 !important; border-radius: 8px !important; 
                }
                div[data-testid="stDataFrame"] div[role="grid"] { color: #000; }
                div[data-testid="stDialog"] { backdrop-filter: blur(5px); background-color: rgba(0, 0, 0, 0.4); }
                div[data-testid="stAlert"] > div, div[data-testid="stAlert"] p { color: #000000 !important; font-weight: 500 !important; }

                /* --- BOTÕES GERAIS (ANIMAÇÃO) --- */
                div.stButton > button {
                    transition: transform 0.2s ease-in-out, box-shadow 0.2s ease-in-out !important;
                }
                div.stButton > button:hover {
                    transform: scale(1.02) !important;
                    z-index: 2;
                }

                /* --- BOTÃO PRIMÁRIO (Azul) --- */
                div.stButton > button[kind="primary"]:not([key^="sub_"]) { 
                    background: linear-gradient(135deg, #0047AB 0%, #000000 150%) !important; 
                    color: white !important; font-weight: 700 !important; border: none !important; 
                }

                /* --- BOTÃO SECUNDÁRIO --- */
                section[data-testid="stMain"] div.stButton > button[kind="secondary"] {
                    border: 1px solid #CCCCCC !important;
                    color: #000000 !important;
                    background-color: #F0F2F6 !important;
                    font-weight: 600 !important;
                }
                
                section[data-testid="stMain"] div.stButton > button[kind="secondary"]:hover {
                    background-color: #DCDFE4 !important; 
                    color: #0047AB !important; /* Texto fica azul apenas no hover */
                    border-color: #0047AB !important;
                    box-shadow: 0 4px 6px rgba(0,0,0,0.1) !important;
                }
                
                    /* Aplicação do Hover Condicional */
                    {hover_secondary}
                    
            </style>
        """, unsafe_allow_html=True)

    def render_sidebar(self):
        with st.sidebar:
            st.markdown('<div style="padding:10px 0px;"><h2 style="color:#CC0000 !important; font-weight:900;">REDE <span style="color:#0047AB;">ESPAFER</span></h2></div>', unsafe_allow_html=True)
            
            with st.expander("PEDIDO DE COMPRA", expanded=True):
                opcoes = ["Gerar Cobertura", "Gerar Orçamento", "Inteligência de Compra"]
                for opt in opcoes:
                    is_active = st.session_state.menu_ativo == opt
                    tipo_botao = "primary" if is_active else "secondary"
                    if st.button(opt, key=f"sub_{opt}", type=tipo_botao, use_container_width=True):
                        st.session_state.menu_ativo = opt
                        st.rerun()

            # --- FILTROS (Mantenha dentro do with st.sidebar) ---
            if "dados_completos" in st.session_state:
                df_ref = st.session_state.dados_completos
                st.markdown("---")
                st.markdown("### 🔍 Filtros")

                # Categoria (Marcas)
                opcoes_marcas = sorted(df_ref['marca'].dropna().unique())
                marcas = st.multiselect("Marcas:", opcoes_marcas, key="f_marca")

                # Subcategoria dinâmica
                if marcas:
                    df_sub = df_ref[df_ref['marca'].isin(marcas)]
                    opcoes_sub = sorted(df_sub['nome_sub_grupo'].dropna().unique())
                else:
                    opcoes_sub = sorted(df_ref['nome_sub_grupo'].dropna().unique())

                subs = st.multiselect("Subcategorias:", opcoes_sub, key="f_sub")
                
                # IMPORTANTE: Guardar para usar na tela
                st.session_state['filtro_marca'] = marcas
                st.session_state['filtro_sub'] = subs

    def salvar_edicoes(self):
        if "editor_orcamento_grid" in st.session_state:
            state = st.session_state["editor_orcamento_grid"]
            edited_rows = state.get("edited_rows", {})
            
            df_exibido = st.session_state.get('df_view_atual')
            
            if df_exibido is not None and not df_exibido.empty:
                for idx_visual, changes in edited_rows.items():
                    try:
                        idx_int = int(idx_visual)
                        id_produto = df_exibido.loc[idx_int, 'idproduto']
                        
                        mask = st.session_state.dados_orcamento['idproduto'] == id_produto
                        
                        for col, value in changes.items():
                            st.session_state.dados_orcamento.loc[mask, col] = value
                    except Exception as e:
                        logger.error(f"Erro ao salvar edição: {e}")

    # --- GERADOR DE PDF (COTAÇÃO) ---
    def gerar_pdf_cotacao(self, fornecedor, grupo_itens):
        # Inicializa o PDF (Certifique-se que a classe PDF está configurada para azul Espafer)
        pdf = PDF(fornecedor=fornecedor) 
        pdf.add_page()
        
        # --- CONFIGURAÇÕES DE CORES (PALETA ESPAFER) ---
        AZUL_ESCURO = (0, 71, 171)   # Azul Royal (Rede Espafer)
        AZUL_CLARO = (235, 245, 255) # Fundo das linhas pares
        CINZA_TEXTO = (50, 50, 50)
        BRANCO = (255, 255, 255)

        # --- CABEÇALHO DA TABELA ---
        pdf.set_font('Arial', 'B', 10)
        pdf.set_fill_color(*AZUL_ESCURO)
        pdf.set_text_color(*BRANCO)
        pdf.set_draw_color(*AZUL_ESCURO) # Borda na cor do azul
        
        # Altura da célula aumentada para 10 para ficar mais elegante
        pdf.cell(30, 10, 'CÓDIGO', 1, 0, 'C', 1)
        pdf.cell(130, 10, '  DESCRIÇÃO DO PRODUTO', 1, 0, 'L', 1)
        pdf.cell(30, 10, 'QTD', 1, 1, 'C', 1)

        # --- CORPO DA TABELA ---
        pdf.set_font('Arial', '', 9)
        pdf.set_text_color(*CINZA_TEXTO)
        
        fill = False 
        for index, row in grupo_itens.iterrows():
            # Alternância de cores (Visual Zebra moderno)
            if fill:
                pdf.set_fill_color(*AZUL_CLARO)
            else:
                pdf.set_fill_color(*BRANCO)
            
            # Sanitização de texto para evitar erro de encoding no PDF
            raw_prod = str(row['produto']).upper() # Produtos em caixa alta ficam mais profissionais em cotações
            raw_prod = raw_prod.replace('\u2013', '-').replace('\u2014', '-')
            produto_nome_sanitizado = raw_prod.encode('latin-1', 'replace').decode('latin-1')
            
            # Truncar nome se for muito longo para não quebrar a linha
            produto_nome = produto_nome_sanitizado[:70] + '...' if len(produto_nome_sanitizado) > 70 else produto_nome_sanitizado
            
            # Desenhando as células
            # 'B' no final garante apenas borda inferior para um visual mais limpo e aberto
            pdf.cell(30, 9, str(row['idproduto']), 'B', 0, 'C', True)
            pdf.cell(130, 9, '  ' + produto_nome, 'B', 0, 'L', True)
            
            # Formatação da quantidade (Bold para destacar)
            pdf.set_font('Arial', 'B', 9)
            pdf.cell(30, 9, f"{row['Qtd Compra']:.0f}  ", 'B', 1, 'R', True) # Alinhado à direita com pequeno respiro
            pdf.set_font('Arial', '', 9) # Volta para fonte normal
            
            fill = not fill 
        
        # --- RODAPÉ DA TABELA ---
        pdf.ln(5)
        pdf.set_font('Arial', 'I', 8)
        pdf.set_text_color(100, 100, 100)
        data_atual = datetime.datetime.now().strftime('%d/%m/%Y %H:%M')
        pdf.cell(190, 10, f'Documento gerado automaticamente pelo Sistema Rede Espafer em {data_atual}', 0, 0, 'R')

        return pdf.output(dest='S').encode('latin-1', 'replace')

    # --- GERADOR DE PDF (SOBRAS) ---
    def gerar_pdf_sobra(self, df, dias_corte):
        # Ordenação e preparação da mensagem
        df = df.sort_values(by='vol_estoque', ascending=False)
        msg_filtro = f"Filtro aplicado: Itens com cobertura superior a {dias_corte} dias."

        # Inicializa o PDF com a paleta da Rede Espafer
        pdf = PDF(
            titulo="RELATÓRIO DE SOBRA DE ESTOQUE", 
            subtitulo="Ordenado por Volume de Estoque (Decrescente)", 
            fornecedor=None,
            info_extra=msg_filtro
        )
        pdf.add_page()

        # --- CONFIGURAÇÕES DE CORES (PALETA ESPAFER) ---
        AZUL_ESCURO = (0, 71, 171)   # Azul Royal
        AZUL_CLARO = (235, 245, 255) # Fundo Zebra
        CINZA_TEXTO = (40, 40, 40)
        BRANCO = (255, 255, 255)

        # --- CABEÇALHO DA TABELA ---
        pdf.set_font('Arial', 'B', 9)
        pdf.set_fill_color(*AZUL_ESCURO)
        pdf.set_text_color(*BRANCO)
        pdf.set_draw_color(*AZUL_ESCURO)

        # Altura 9 para um cabeçalho robusto
        pdf.cell(15, 9, 'LOJA', 1, 0, 'C', 1)
        pdf.cell(105, 9, '  DESCRIÇÃO DO PRODUTO', 1, 0, 'L', 1) # Ajustado para 105 para dar espaço às outras
        pdf.cell(35, 9, 'ESTOQUE ATUAL', 1, 0, 'C', 1)
        pdf.cell(35, 9, 'VENDA (30D)', 1, 1, 'C', 1)

        # --- CORPO DA TABELA ---
        pdf.set_font('Arial', '', 8)
        pdf.set_text_color(*CINZA_TEXTO)

        fill = False 
        for index, row in df.iterrows():
            # Zebra modernizada
            if fill:
                pdf.set_fill_color(*AZUL_CLARO)
            else:
                pdf.set_fill_color(*BRANCO)

            # Sanitização e tratamento do nome
            raw_prod = str(row['produto']).upper()
            raw_prod = raw_prod.replace('\u2013', '-').replace('\u2014', '-')
            produto_nome_sanitizado = raw_prod.encode('latin-1', 'replace').decode('latin-1')

            # Truncar nome para não sobrepor colunas
            produto_nome = produto_nome_sanitizado[:60] + '...' if len(produto_nome_sanitizado) > 60 else produto_nome_sanitizado

            # Desenhando as células com borda apenas inferior ('B')
            pdf.cell(15, 8, str(row['filial']), 'B', 0, 'C', True)
            pdf.cell(105, 8, '  ' + produto_nome, 'B', 0, 'L', True)

            # Valores de Estoque e Venda (Estoque em Negrito para destaque)
            pdf.set_font('Arial', 'B', 8)
            pdf.cell(35, 8, f"{row['vol_estoque']:.0f} und ", 'B', 0, 'R', True)

            pdf.set_font('Arial', '', 8)
            pdf.cell(35, 8, f"{row['venda_periodo']:.1f}  ", 'B', 1, 'R', True)

            fill = not fill 

        # --- RODAPÉ ---
        pdf.ln(5)
        pdf.set_font('Arial', 'I', 7)
        pdf.set_text_color(120, 120, 120)
        data_atual = datetime.datetime.now().strftime('%d/%m/%Y %H:%M')
        pdf.cell(190, 8, f'Relatório de Inteligência de Estoque - Rede Espafer - Gerado em: {data_atual}', 0, 0, 'R')

        return pdf.output(dest='S').encode('latin-1', 'replace')
    
    def gerar_pdf_pedido(self, fornecedor, df_pedido):
        pdf = FPDF()
        pdf.add_page()
        pdf.set_font("Arial", "B", 16)

        # Cabeçalho
        pdf.cell(190, 10, f"PEDIDO DE COMPRA - REDE ESPAFER", ln=True, align='C')
        pdf.set_font("Arial", "", 12)
        pdf.cell(190, 10, f"Fornecedor: {fornecedor}", ln=True, align='L')
        pdf.cell(190, 10, f"Data: {datetime.datetime.now().strftime('%d/%m/%Y %H:%M')}", ln=True, align='L')
        pdf.ln(10)

        # Tabela - Cabeçalho
        pdf.set_fill_color(200, 220, 255)
        pdf.set_font("Arial", "B", 10)
        pdf.cell(30, 8, "Código", 1, 0, 'C', True)
        pdf.cell(80, 8, "Produto", 1, 0, 'C', True)
        pdf.cell(25, 8, "Qtd", 1, 0, 'C', True)
        pdf.cell(25, 8, "V. Unit", 1, 0, 'C', True)
        pdf.cell(30, 8, "Total", 1, 1, 'C', True)

        # Tabela - Dados
        pdf.set_font("Arial", "", 9)
        total_geral = 0
        for _, row in df_pedido.iterrows():
            pdf.cell(30, 7, str(row['Código']), 1)
            pdf.cell(80, 7, str(row['Produto'])[:40], 1) # Limita nome longo
            pdf.cell(25, 7, str(row['Quantidade']), 1, 0, 'C')
            pdf.cell(25, 7, f"R$ {row['Valor Unitário']:.2f}", 1, 0, 'R')
            pdf.cell(30, 7, f"R$ {row['Total']:.2f}", 1, 1, 'R')
            total_geral += row['Total']

        # Rodapé Total
        pdf.set_font("Arial", "B", 11)
        pdf.cell(160, 10, "VALOR TOTAL DO PEDIDO:", 0, 0, 'R')
        pdf.cell(30, 10, f"R$ {total_geral:.2f}", 0, 1, 'R')

        return pdf.output(dest='S').encode('latin-1') # Retorna os bytes do PDF

    # --- EXCEL ATUALIZADO (IMPOSTOS, FRETE, PRAZO) ---
    def gerar_excel_cotacao(self, df_input):
        output = io.BytesIO()
        
        # 1. Prepara os dados iniciais (Apenas o que já temos)
        # Vamos criar o DataFrame base apenas com as colunas fixas de origem
        df_export = df_input[['idproduto', 'produto', 'Qtd Compra']].copy()
        
        # Renomeia para o cabeçalho final
        df_export.columns = ['Código', 'Produto', 'Quantidade']
        
        # 2. Configura o Excel Writer
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            # Escreve os dados iniciais (pandas trata as 3 primeiras colunas)
            df_export.to_excel(writer, index=False, sheet_name='Cotacao')
            
            workbook = writer.book
            worksheet = writer.sheets['Cotacao']
            
            # --- Definição de Formatos ---
            
            # Formato CINZA para campos editáveis (Solicitado)
            fmt_editavel_cinza = workbook.add_format({
                'bg_color': '#D3D3D3', 
                'border': 1,
                'num_format': '#,##0.00'
            })
            
            # Formato para o Total (Negrito, calculado)
            fmt_total = workbook.add_format({
                'bold': True, 
                'border': 1, 
                'num_format': 'R$ #,##0.00',
                'bg_color': '#F2F2F2' # Um cinza bem clarinho só pra destacar
            })
            
            # Formato básico para bordas nas colunas de texto
            fmt_texto = workbook.add_format({'border': 1})
            fmt_numero = workbook.add_format({'border': 1, 'num_format': '0'})

            # --- Cabeçalhos das Colunas Novas ---
            # O Pandas escreveu as colunas 0, 1 e 2. Vamos escrever os cabeçalhos das 3 a 7 manualmente.
            header_format = workbook.add_format({'bold': True, 'border': 1, 'bg_color': '#E0E0E0'})
            
            colunas_extras = ['Valor Unitário', 'Impostos', 'Frete', 'Prazo de Entrega', 'Valor Total']
            for idx, col_name in enumerate(colunas_extras):
                # Começa na coluna 3 (D)
                worksheet.write(0, 3 + idx, col_name, header_format)

            # --- Ajuste de Largura e Filtros ---
            
            # Aplica Filtro em TODAS as colunas (A até H -> 0 a 7)
            worksheet.autofilter(0, 0, len(df_export), 7)
            
            worksheet.set_column('A:A', 10) # Código
            worksheet.set_column('B:B', 40) # Produto
            worksheet.set_column('C:C', 12) # Quantidade
            worksheet.set_column('D:D', 15) # Vl Unit (Cinza)
            worksheet.set_column('E:E', 12) # Impostos (Cinza)
            worksheet.set_column('F:F', 12) # Frete (Cinza)
            worksheet.set_column('G:G', 18) # Prazo (Cinza)
            worksheet.set_column('H:H', 18) # Total

            # --- Loop para preencher células e fórmulas ---
            for i in range(len(df_export)):
                row = i + 1 # Linha do Excel (1-based, pulando header)
                
                # Reaplica borda nas colunas A, B, C que o Pandas escreveu
                worksheet.write(row, 0, df_export.iloc[i, 0], fmt_texto)  # Código
                worksheet.write(row, 1, df_export.iloc[i, 1], fmt_texto)  # Produto
                worksheet.write(row, 2, df_export.iloc[i, 2], fmt_numero) # Quantidade
                
                # Colunas Editáveis (CINZA) - Escrevemos em branco
                worksheet.write_blank(row, 3, None, fmt_editavel_cinza) # D: Vl Unit
                worksheet.write_blank(row, 4, None, fmt_editavel_cinza) # E: Impostos
                worksheet.write_blank(row, 5, None, fmt_editavel_cinza) # F: Frete
                worksheet.write_blank(row, 6, None, fmt_editavel_cinza) # G: Prazo
                
                # Coluna Valor Total (H) - FÓRMULA
                # Lógica: (Qtd * Vl Unit) + Impostos + Frete
                # Colunas: C=Qtd, D=Unit, E=Imp, F=Frete
                # Excel row é row+1
                formula = f'=(C{row+1}*D{row+1})+E{row+1}+F{row+1}'
                worksheet.write_formula(row, 7, formula, fmt_total)

        output.seek(0)
        return output

    def enviar_email_com_anexo(self, destinatario, assunto, mensagem, anexos):
        """Envia email com anexos usando SMTP."""
        if "email" not in st.secrets:
            logger.error("Configuração [email] não encontrada em secrets.toml")
            return False, "Configuração de e-mail ausente"

        email_config = st.secrets["email"]
        dest_log = _mascarar_email(destinatario)  # Nunca loga o e-mail completo

        try:
            msg = MIMEMultipart()
            msg['From'] = email_config["sender_email"]
            msg['To'] = destinatario
            msg['Subject'] = assunto

            msg.attach(MIMEText(mensagem, 'html'))

            for anexo in anexos:
                try:
                    blob = anexo['dados'].getvalue() if hasattr(anexo['dados'], 'getvalue') else anexo['dados']
                    part = MIMEApplication(blob, Name=anexo['nome'])
                    part['Content-Disposition'] = f'attachment; filename="{anexo["nome"]}"'
                    msg.attach(part)
                    logger.info(f"Anexo adicionado: {anexo['nome']}")
                except Exception as e:
                    logger.error(f"Erro ao adicionar anexo {anexo['nome']}: {e}")
                    return False, f"Erro ao preparar o anexo {anexo['nome']}"

            logger.info(f"Enviando e-mail para {dest_log} — assunto: {assunto}")
            server = smtplib.SMTP(email_config["smtp_server"], int(email_config.get("smtp_port", 587)))
            server.starttls()
            server.login(email_config["sender_email"], email_config["sender_password"])
            server.send_message(msg)
            server.quit()
            logger.info(f"E-mail enviado com sucesso para {dest_log}")
            return True, "Enviado"

        except smtplib.SMTPAuthenticationError:
            logger.error(f"Erro de autenticação SMTP ao enviar para {dest_log}")
            return False, "Falha na autenticação do servidor de e-mail"
        except smtplib.SMTPException as e:
            logger.error(f"Erro SMTP ao enviar para {dest_log}: {e}")
            return False, "Erro no envio do e-mail. Verifique as configurações SMTP."
        except Exception as e:
            logger.error(f"Erro inesperado ao enviar e-mail para {dest_log}: {e}", exc_info=True)
            return False, "Erro inesperado no envio. Contate o suporte."

    # --- TELA DE ORÇAMENTO  ---
    def tela_orcamento(self):
        st.markdown(f'<h1 style="color:black; font-weight:900;">{st.session_state.menu_ativo}</h1>', unsafe_allow_html=True)

        # 1. VERIFICAÇÃO INICIAL
        if st.session_state.dados_orcamento is None or st.session_state.dados_orcamento.empty:
            st.warning("Nenhum produto selecionado para orçamento.")
            return

        # 2. BUSCA DE FORNECEDORES
        df_forn_db = self.db.buscar_fornecedores() 
        if df_forn_db.empty:
            st.error("A tabela de fornecedores está vazia.")
            return

        # Mapeamento de contatos
        df_forn_db['fornecedor'] = df_forn_db['fornecedor'].astype(str).str.strip()
        df_forn_db['marca_aux'] = df_forn_db['marca'].astype(str).str.strip().str.upper()

        st.session_state.db_fornecedores = {
            row['fornecedor']: {'email': row['email']} 
            for _, row in df_forn_db.iterrows() 
            if row['fornecedor'].lower() not in ['none', 'nan', '', 'null']
        }
        lista_geral_fornecedores = sorted(list(st.session_state.db_fornecedores.keys()))

        # 3. PREPARAÇÃO DE DADOS E FILTROS
        df_master = st.session_state.dados_orcamento.copy()
        df_master['marca_aux'] = df_master['marca'].astype(str).str.strip().str.upper()
        df_master['cat_filtro'] = df_master['grupo'].fillna("SEM GRUPO").astype(str).str.upper() if 'grupo' in df_master.columns else "SEM GRUPO"

        mapa_marca_fornecedor = df_forn_db.groupby('marca_aux')['fornecedor'].apply(
            lambda x: list(set(i for i in x if i.lower() not in ['none', 'nan', '']))
        ).to_dict()

        cats = sorted([str(c) for c in df_master['cat_filtro'].unique() if str(c).strip() not in ["", "None", "nan"]])

        c1, c2, c3, c4 = st.columns([1, 1.5, 1.5, 1.5], vertical_alignment="bottom")
        with c1: filtro_positivo = st.selectbox("Filtrar Grupo:", options=["TODOS"] + cats)
        with c2: grupos_remover = st.multiselect("Remover Grupo:", options=cats)
        with c3: termo_busca = st.text_input("Pesquisar Produto:", key="search_orc")
        with c4:
            modo_envio = st.selectbox("Enviar Para:", options=["Manual (Sugerir p/ Marca)", "TODOS (Cotação Aberta)"] + lista_geral_fornecedores)
            enviar_todos_forn = (modo_envio == "TODOS (Cotação Aberta)")

        # Lógica de Sugestão de Fornecedor
        if enviar_todos_forn:
            df_master['Fornecedor'] = [["TODOS"]] * len(df_master)
        elif modo_envio != "Manual (Sugerir p/ Marca)":
            df_master['Fornecedor'] = [[modo_envio]] * len(df_master)
        else:
            def sugerir_um_fornecedor(marca):
                opcoes = mapa_marca_fornecedor.get(marca, [])
                return [opcoes[0]] if opcoes else ([lista_geral_fornecedores[0]] if lista_geral_fornecedores else [])
            df_master['Fornecedor'] = df_master['marca_aux'].apply(sugerir_um_fornecedor)

        st.session_state.dados_orcamento = df_master

        # 4. MENSAGEM DE DICA E TABELA
        st.info("💡 **Dica:** Campos com '✏️' são editáveis (Fornecedor e Quantidade).")

        df_view = df_master.copy()
        # (Filtros de visualização aplicados aqui...)
        df_view = df_view.reset_index(drop=True)

        st.data_editor(
            df_view[['idproduto', 'produto', 'cat_filtro', 'Fornecedor', 'Qtd Compra']],
            column_config={
                "idproduto": st.column_config.TextColumn("Cód.", disabled=True),
                "produto": st.column_config.TextColumn("Produto", disabled=True, width="large"),
                "Fornecedor": st.column_config.MultiselectColumn("✏️ Fornecedor", options=lista_geral_fornecedores),
                "Qtd Compra": st.column_config.NumberColumn("✏️ Quantidade", min_value=0),
            },
            hide_index=True, use_container_width=True, key="editor_orcamento_grid", on_change=self.salvar_edicoes
        )

        st.divider()

        # 5. BOTÃO DE ENVIO COM GERAÇÃO DE EXCEL PROFISSIONAL
        if st.button("📨 ENVIAR COTAÇÃO", type="primary"):
            df_final_envio = st.session_state.dados_orcamento.copy()
            itens_para_compra = df_final_envio[df_final_envio['Qtd Compra'] > 0].copy()
        
            if itens_para_compra.empty:
                st.warning("Preencha a quantidade de envio.")
            else:
                with st.spinner("Gerando planilhas profissionais e enviando..."):
                    envios_sucesso = 0
                    # Identifica fornecedores únicos que possuem itens para comprar
                    fornecedores_alvo = set()
                    for lista in itens_para_compra['Fornecedor']:
                        if isinstance(lista, list): fornecedores_alvo.update(lista)
        
                    for forn in fornecedores_alvo:
                        # Filtra produtos deste fornecedor
                        df_forn = itens_para_compra[itens_para_compra['Fornecedor'].apply(lambda x: forn in x if isinstance(x, list) else False)]
                        email_dest = st.session_state.db_fornecedores.get(forn, {}).get("email")
        
                        if email_dest:
                            # --- 1. GERAÇÃO DO EXCEL COM TODAS AS CONFIGURAÇÕES ---
                            buffer_excel = io.BytesIO()
                            with pd.ExcelWriter(buffer_excel, engine='xlsxwriter') as writer:
                                workbook = writer.book
                                worksheet = workbook.add_worksheet('Cotacao')
                                
                                # FORMATOS (Igual ao seu código original)
                                fmt_cabecalho = workbook.add_format({
                                    'bold': True, 'text_wrap': True, 'valign': 'top', 'fg_color': '#EEEEEE', 'border': 1
                                })
                                fmt_texto = workbook.add_format({'border': 1})
                                fmt_numero = workbook.add_format({'border': 1, 'align': 'center'})
                                
                                # Estilo CINZA para entrada de dados (Onde o fornecedor digita)
                                fmt_cinza_input = workbook.add_format({
                                    'bg_color': '#D3D3D3', 'border': 1, 'num_format': 'R$ #,##0.00'
                                })
                                
                                # Estilo TOTAL com fórmula e negrito
                                fmt_total = workbook.add_format({
                                    'num_format': 'R$ #,##0.00', 'border': 1, 'bold': True
                                })
                                fmt_data = workbook.add_format({
                                    'bg_color': '#D3D3D3', 
                                    'border': 1, 
                                    'num_format': 'dd/mm/yyyy',
                                    'align': 'center'
                                })
        
                                # CABEÇALHOS
                                colunas = ['Cód.', 'Produto', 'Qtd.', 'Valor Unitário', 'Valor Total', 'Prazo Entrega']
                                for col_num, valor in enumerate(colunas):
                                    worksheet.write(0, col_num, valor, fmt_cabecalho)
        
                                # LINHAS COM DADOS E FÓRMULAS
                                for row_idx, row_data in df_forn.reset_index(drop=True).iterrows():
                                    excel_row = row_idx + 1 
                                    
                                    worksheet.write(excel_row, 0, str(row_data['idproduto']), fmt_texto)
                                    worksheet.write(excel_row, 1, str(row_data['produto']), fmt_texto)
                                    worksheet.write(excel_row, 2, row_data['Qtd Compra'], fmt_numero)
                                    
                                    # Célula em CINZA para o fornecedor preencher
                                    worksheet.write_blank(excel_row, 3, None, fmt_cinza_input)
                                    worksheet.write_blank(excel_row, 5, None, fmt_data)
                                    
                                    # Fórmula automática: Qtd * Valor Unitário
                                    linha_excel = excel_row + 1
                                    formula = f'=C{linha_excel}*D{linha_excel}'
                                    worksheet.write_formula(excel_row, 4, formula, fmt_total)
        
                                # AJUSTES DE LARGURA (Configuração Original)
                                worksheet.set_column('A:A', 10) # Cód
                                worksheet.set_column('B:B', 50) # Produto
                                worksheet.set_column('C:C', 8)  # Qtd
                                worksheet.set_column('D:D', 15) # Valor Unit
                                worksheet.set_column('E:E', 18) # Total
                                
                                # Filtro Automático
                                worksheet.autofilter(0, 0, len(df_forn), 4)
        
                            excel_bytes = buffer_excel.getvalue()
        
                            # --- 2. ASSUNTO E MENSAGEM HTML ---
                            assunto = "Solicitação de Cotação - Rede Espafer"
                            mensagem_html = f"""
                            <div style="font-family: Arial, sans-serif;">
                                <p>Olá <strong>{forn}</strong>,</p>
                                <p>Segue solicitação de cotação da Rede Espafer.</p>
                                <p><strong>Instruções:</strong></p>
                                <ul>
                                    <li>Abra a planilha Excel em anexo;</li>
                                    <li>Preencha as células em <strong>CINZA</strong>;</li>
                                    <li>Salve e nos responda com este arquivo preenchido.</li>
                                </ul>
                                <p>Atenciosamente,<br>
                                <strong>Compras - Rede Espafer</strong></p>
                            </div>
                            """
        
                            # --- 3. ENVIO COM O FORMATO DE ANEXO CORRETO ---
                            sucesso, _ = self.enviar_email_com_anexo(
                                email_dest,
                                assunto,
                                mensagem_html,
                                [{'nome': f'Cotacao_Espafer_{forn}.xlsx', 'dados': excel_bytes}]
                            )
                            
                            if sucesso:
                                envios_sucesso += 1
        
                    st.success(f"✅ Enviado com sucesso para {envios_sucesso} fornecedores!")

    def tela_cobertura(self):
        # 1. CSS Unificado
        st.markdown("""
            <style>
                [data-baseweb="select"] * { color: #000000 !important; -webkit-text-fill-color: #000000 !important; }
                .align-btn { margin-top: 28px; }
            </style>
        """, unsafe_allow_html=True)

        with st.container():
            # --- FILTROS SUPERIORES EM CASCATA ---
            c1, c2, c3 = st.columns(3) 
            c4, c5, c6 = st.columns(3)

            lista_filiais = self.db.buscar_filiais()
            sel_filial = c1.selectbox("Filial", options=["TODOS"] + lista_filiais, key="v6_filial")
            v_filial = sel_filial if sel_filial != "TODOS" else None

            marcas = self.db.buscar_marcas(v_filial)
            sel_marca = c2.selectbox("Marca", options=["TODOS"] + marcas, key="v6_marca")
            v_marca = sel_marca if sel_marca != "TODOS" else None

            grupos = self.db.buscar_grupos(v_filial, v_marca)
            sel_grupo = c3.selectbox("Grupo", options=["TODOS"] + grupos, key="v6_grupo")
            v_grupo = sel_grupo if sel_grupo != "TODOS" else None

            subgrupos = self.db.buscar_subgrupos(v_filial, v_marca, v_grupo)
            sel_subgrupo = c4.selectbox("SubGrupo", options=["TODOS"] + subgrupos, key="v6_sub")
            v_subgrupo = sel_subgrupo if sel_subgrupo != "TODOS" else None

            subgrupos1 = self.db.buscar_subgrupos1(v_filial, v_marca, v_grupo, v_subgrupo)
            sel_subgrupo1 = c5.selectbox("SubGrupo1", options=["TODOS"] + subgrupos1, key="v6_sub1")
            v_subgrupo1 = sel_subgrupo1 if sel_subgrupo1 != "TODOS" else None

            produtos = self.db.buscar_produtos(v_filial, v_marca, v_grupo, v_subgrupo, v_subgrupo1)
            sel_produto = c6.selectbox("Produto", options=["TODOS"] + produtos, key="v6_prod")
            v_produto = sel_produto if sel_produto != "TODOS" else None

            st.write("") 

            # --- PARÂMETROS DE ANÁLISE ---
            c_tipo, c_p1, c_p2, c_btn = st.columns([1.5, 1, 1, 1.5])

            # Inicializa a variável para evitar erro de referência
            incluir_sem_venda = False

            with c_tipo:
                modo = st.selectbox("Tipo de Análise", ["Sugestão de Compra", "Análise de Sobra"])

            if modo == "Sugestão de Compra":
                with c_p1: dias_alvo = st.number_input("Cobertura Alvo (Dias)", min_value=1, value=30)
                with c_p2: dias_corte = st.number_input("Estoque Mínimo (Dias)", min_value=0, value=10)
                modo_db = 'COMPRA'
            else:
                dias_alvo = 30
                with c_p1: dias_corte = st.number_input("Estoque Máximo (Dias)", min_value=0, value=45)
                with c_p2:
                    st.markdown("<div style='height: 33px;'></div>", unsafe_allow_html=True)
                    # O checkbox deve vir antes da lógica do botão
                    incluir_sem_venda = st.checkbox("Incluir não vendidos?", key="chk_venda_v6", value=False)
                modo_db = 'SOBRA'

            # --- BOTÃO GERAR ANÁLISE ---
            with c_btn:
                st.markdown('<div class="align-btn"></div>', unsafe_allow_html=True)
                if st.button("GERAR ANÁLISE", type="primary", use_container_width=True):
                    with st.spinner("Processando..."):
                        df = self.db.consultar_cobertura(
                            v_filial, v_marca, v_grupo, v_subgrupo, v_subgrupo1, v_produto, 
                            dias_alvo, dias_corte, modo_db
                        )

                        # --- LÓGICA DE FILTRO DE VENDAS ---
                        if not df.empty:
                            # Se NÃO marcar "incluir não vendidos", removemos itens com venda_periodo <= 0
                            if not incluir_sem_venda:
                                df = df[df['venda_periodo'] > 0]

                        st.session_state.df_analise_cache = df
                        st.session_state.modo_analise_atual = modo_db
                        if len(df) == 2000:
                            st.warning("⚠️ O resultado foi limitado a 2.000 itens. Aplique filtros mais específicos para ver todos os dados.")
                        st.rerun()

            # --- PROCESSAMENTO DOS DADOS PARA EXIBIÇÃO ---
            df_cache = st.session_state.get('df_analise_cache', pd.DataFrame())
            modo_atual = st.session_state.get('modo_analise_atual', 'COMPRA')

            if not df_cache.empty:
                st.divider()
                df_processado = df_cache.copy()

                # 1. ORDENAÇÃO
                if modo_atual == 'SOBRA':
                    df_processado = df_processado.sort_values(by=['dias_estoque', 'vol_estoque'], ascending=[False, False])
                else:
                    df_processado = df_processado.sort_values(by=['dias_estoque', 'reposicao'], ascending=[True, False])

                df_processado = df_processado.drop_duplicates(subset=['idproduto'], keep='first')

                # 2. DEFINIÇÃO DE COLUNAS
                lista_base = ['filial', 'idproduto', 'produto', 'marca', 'vol_estoque', 'venda_periodo', 'dias_estoque', 'reposicao']
                ordem_visual = [c for c in lista_base if c != 'reposicao'] if modo_atual == 'SOBRA' else lista_base

                colunas_finais = [c for c in ordem_visual if c in df_processado.columns]
                outras_colunas = [c for c in df_processado.columns if c not in colunas_finais]
                df_final = df_processado[colunas_finais + outras_colunas]

                # 3. ESTILIZAÇÃO
                def aplicar_cores_e_formatos(styler):
                    if modo_atual == 'SOBRA':
                        styler.background_gradient(cmap='Reds', subset=['dias_estoque'])
                    else:
                        styler.background_gradient(cmap='Reds_r', subset=['dias_estoque'], vmin=0, vmax=30)
                        if 'reposicao' in df_final.columns:
                            styler.background_gradient(cmap='Blues', subset=['reposicao'], vmin=0, vmax=100)

                    formatos = {'vol_estoque': '{:.0f} und', 'venda_periodo': '{:.0f} und', 'dias_estoque': '{:.1f} dias'}
                    if 'reposicao' in df_final.columns and modo_atual != 'SOBRA':
                        formatos['reposicao'] = '{:.0f} und'
                    return styler.format(formatos)

                st.dataframe(
                    aplicar_cores_e_formatos(df_final.style),
                    use_container_width=True, hide_index=True, height=600,
                    column_config={"grupo": None, "subgrupo": None, "subgrupo1": None, "nome_empresa_completo": None}
                )

                # --- RODAPÉ (AÇÕES) ---
                col_espaco, col_acao = st.columns([3, 2])
                with col_acao:
                    if modo_atual == 'SOBRA':
                        try:
                            from reportlab.lib.pagesizes import A4, landscape
                            from reportlab.lib import colors
                            from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
                            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
                            from reportlab.lib.units import cm
                            import io

                            output = io.BytesIO()
                            doc = SimpleDocTemplate(output, pagesize=landscape(A4), 
                                                    rightMargin=1*cm, leftMargin=1*cm, 
                                                    topMargin=1*cm, bottomMargin=1*cm)

                            elementos = []
                            estilos = getSampleStyleSheet()
                            estilo_titulo = ParagraphStyle('T1', parent=estilos['Title'], fontSize=16, spaceAfter=10)
                            estilo_celula = ParagraphStyle('Cel', parent=estilos['Normal'], fontSize=8, leading=10, wordWrap='CJK')
                            estilo_header = ParagraphStyle('Hdr', parent=estilos['Normal'], fontSize=9, textColor=colors.whitesmoke, fontName='Helvetica-Bold', alignment=1)

                            elementos.append(Paragraph("RELATÓRIO DE SOBRAS DE ESTOQUE", estilo_titulo))
                            elementos.append(Paragraph(f"Data: {pd.Timestamp.now().strftime('%d/%m/%Y %H:%M')}", estilos['Normal']))
                            elementos.append(Spacer(1, 0.5*cm))

                            cols_pdf = [c for c in ['filial', 'idproduto', 'produto', 'marca', 'vol_estoque', 'venda_periodo', 'dias_estoque'] if c in df_final.columns]
                            
                            dados_pdf = [[Paragraph(c.upper(), estilo_header) for c in cols_pdf]]

                            for _, row in df_final.iterrows():
                                linha = []
                                for col in cols_pdf:
                                    val = row[col]
                                    texto = f"{val:.1f}" if isinstance(val, (float, int)) and col not in ['produto', 'filial', 'idproduto'] else str(val)
                                    linha.append(Paragraph(texto, estilo_celula))
                                dados_pdf.append(linha)

                            tabela = Table(dados_pdf, repeatRows=1)
                            tabela.setStyle(TableStyle([
                                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#0047AB")),
                                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.whitesmoke, colors.HexColor("#F2F2F2")]),
                            ]))

                            elementos.append(tabela)
                            doc.build(elementos)

                            st.download_button(
                                label="📄 Baixar PDF de Sobras",
                                data=output.getvalue(),
                                file_name=f"Sobras_{pd.Timestamp.now().strftime('%d%m')}.pdf",
                                mime="application/pdf",
                                type="primary",
                                use_container_width=True
                            )
                        except Exception as e:
                            st.error(f"Erro ao gerar PDF: {str(e)}")
                    
                    else:
                        if st.button("➕ Adicionar TODOS ao Orçamento", type="primary", use_container_width=True):
                            df_orc = df_final.copy()
                            if 'reposicao' in df_orc.columns:
                                df_orc['Qtd Compra'] = df_orc['reposicao']
                            
                            # Correção aqui: adaptando os nomes das colunas para os novos filtros
                            cols_para_orc = ['idproduto', 'produto', 'vol_estoque', 'marca', 'grupo', 'subgrupo', 'venda_periodo', 'dias_estoque', 'Qtd Compra']
                            existentes_orc = [c for c in cols_para_orc if c in df_orc.columns]
                            
                            df_final_orc = df_orc[existentes_orc].copy()
                            df_final_orc['Fornecedor'] = None

                            if st.session_state.dados_orcamento is None or st.session_state.dados_orcamento.empty:
                                st.session_state.dados_orcamento = df_final_orc
                            else:
                                st.session_state.dados_orcamento = pd.concat([st.session_state.dados_orcamento, df_final_orc], ignore_index=True)
                                st.session_state.dados_orcamento.drop_duplicates(subset=['idproduto'], keep='last', inplace=True)

                            st.success(f"{len(df_final_orc)} itens adicionados!")
                            st.session_state.menu_ativo = "Gerar Orçamento"
                            st.rerun()
            
            elif 'df_analise_cache' in st.session_state and st.session_state.df_analise_cache.empty:
                st.warning("Nenhum item encontrado com os filtros selecionados.")

    def tela_analise_retorno(self):
        # 1. Inicializar Estado da Estratégia
        if 'estrategia_ativa' not in st.session_state:
            st.session_state['estrategia_ativa'] = 'menor_preco'

        # 2. CSS ATUALIZADO (Mantido fiel ao seu original)
        st.markdown("""
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;700;900&display=swap');
            
            /* 1. CONFIGURAÇÕES GERAIS */
            .stApp { background-color: #FFFFFF !important; }
            
            /* 2. SIDEBAR - Ajustada para linha AZUL */
            section[data-testid="stSidebar"] { 
                background-color: #121212 !important; 
                border-right: 2px solid #0047AB !important; 
            }
            section[data-testid="stSidebar"] .stButton > button { 
                background-color: transparent !important; color: #FFFFFF !important; 
                border: none !important; width: 100%; text-align: left; 
            }
            section[data-testid="stSidebar"] div:has(button[kind="primary"]) > div > button { 
                background-color: #FFFFFF !important; color: #000000 !important; font-weight: 900 !important; 
            }
            
            /* 3. UPLOAD - Mantendo seu estilo de fundo escuro */
            [data-testid="stFileUploader"] { background-color: #262730; border-radius: 10px; padding: 20px; }
            [data-testid="stFileUploader"] section div { color: white !important; }
            [data-testid="stFileUploader"] label p { color: white !important; }
            [data-testid="stFileUploader"] svg { fill: white !important; }
            div[data-testid="stUploadedFile"] { background-color: #333333; border: 1px solid #555; border-radius: 8px; }
            div[data-testid="stUploadedFile"] span { color: white !important; }
        
            /* 4. CAIXA DE ESTRATÉGIA */
            div[data-testid="stVerticalBlockBorderWrapper"] {
                border: 2px solid #AAAAAA !important;
                border-radius: 8px !important;
                padding: 20px !important;
                background-color: #FFFFFF !important;
            }
        
            /* Botão Secundário (Borda Cinza -> Hover Azul) */
            section[data-testid="stMain"] div.stButton > button[kind="secondary"],
            div[data-testid="stVerticalBlockBorderWrapper"] button[kind="secondary"] {
                background-color: #FFFFFF !important;
                border: 1px solid #CCCCCC !important;
                color: #000000 !important;
                font-weight: 600 !important;
                transition: all 0.2s ease-in-out !important;
            }
        
            /* Efeito Hover */
            section[data-testid="stMain"] div.stButton > button[kind="secondary"]:hover,
            div[data-testid="stVerticalBlockBorderWrapper"] button[kind="secondary"]:hover {
                background-color: #F0F2F6 !important;
                color: #0047AB !important;
                border-color: #0047AB !important;
            }
        
            /* Botão Primário (Azul) / Link de Email */
            div[data-testid="stVerticalBlockBorderWrapper"] button[kind="primary"], .btn-envio-email {
                background: linear-gradient(135deg, #0047AB 0%, #000000 150%) !important;
                border: none !important;
                color: white !important;
                text-decoration: none !important;
                padding: 10px 15px;
                border-radius: 8px;
                display: block;
                text-align: center;
                font-weight: 700;
                font-size: 14px;
            }
        
            /* 6. EXPANDER */
            div[data-testid="stExpander"] > details > summary {
                background: linear-gradient(135deg, #0047AB 0%, #000000 150%) !important;
                color: white !important; font-weight: 900 !important; border-radius: 8px !important;
            }
        </style>
        """, unsafe_allow_html=True)

        st.markdown("""<style>...</style>""", unsafe_allow_html=True)

        st.markdown('<h1 style="color:#000000; font-weight:900;">Inteligência de Compra</h1>', unsafe_allow_html=True)
        
        arquivos = st.file_uploader("Selecione as Cotações dos Fornecedores (.xlsx)", type=["xlsx"], accept_multiple_files=True)
        
        if arquivos:
            try:
                lista_dfs = []
                db_forn = st.session_state.get('db_fornecedores', {})
                fornecedores_cadastrados = list(db_forn.keys())
                tem_prazo = False
                pedidos = {}  # Inicializado no escopo correto

                # --- PROCESSAMENTO DOS ARQUIVOS ---
                for arquivo in arquivos:
                    df_temp = pd.read_excel(arquivo, engine='openpyxl')
                    df_temp.columns = [c.strip() for c in df_temp.columns]

                    # Identificação Inteligente do Fornecedor
                    nome_arquivo_bruto = arquivo.name.upper()
                    termo_busca = nome_arquivo_bruto.replace('.XLSX', '').replace('.XLS', '')
                    for r in ['COTACAO', 'PEDIDO', 'ORCAMENTO', '_', '-']:
                        termo_busca = termo_busca.replace(r, ' ')

                    termo_busca = termo_busca.strip()
                    fornecedor_identificado = None

                    for f_cadastrado in fornecedores_cadastrados:
                        f_limpo = f_cadastrado.upper().strip()
                        if f_limpo in termo_busca or f_limpo.replace(" ", "") in termo_busca.replace(" ", ""):
                            fornecedor_identificado = f_cadastrado
                            break
                        
                    if not fornecedor_identificado:
                        fornecedor_identificado = termo_busca.split(' ')[0] 

                    df_temp['Fornecedor'] = fornecedor_identificado

                    # --- Padronização de Colunas ---
                    # Ordem de prioridade: primeiro identifica colunas mais específicas
                    # para não confundir "Valor Total" com "Valor Unitário"
                    cols_renomeadas = {}
                    for c in df_temp.columns:
                        c_upper = c.upper().strip()

                        # Valor Total (mais específico — verificar ANTES de Valor Unitário)
                        if any(x in c_upper for x in ['TOTAL']) and 'Valor Total' not in cols_renomeadas.values():
                            cols_renomeadas[c] = 'Valor Total'

                        # Valor Unitário (só mapeia se não for "total")
                        elif any(x in c_upper for x in ['UNIT', 'UNITÁRIO', 'UNITARIO', 'PREÇO', 'PRECO']) and 'Valor Unitário' not in cols_renomeadas.values():
                            cols_renomeadas[c] = 'Valor Unitário'

                        # Impostos
                        elif any(x in c_upper for x in ['IMPOSTO', 'IPI', 'ICMS', 'TAX']) and 'Impostos' not in cols_renomeadas.values():
                            cols_renomeadas[c] = 'Impostos'

                        # Frete
                        elif any(x in c_upper for x in ['FRETE']) and 'Frete' not in cols_renomeadas.values():
                            cols_renomeadas[c] = 'Frete'

                        # Prazo
                        elif any(x in c_upper for x in ['PRAZO', 'ENTREGA']) and 'Prazo' not in cols_renomeadas.values():
                            cols_renomeadas[c] = 'Prazo'
                            tem_prazo = True

                    df_temp = df_temp.rename(columns=cols_renomeadas)

                    # Converte colunas numéricas
                    for col_num in ['Valor Unitário', 'Valor Total', 'Impostos', 'Frete']:
                        if col_num in df_temp.columns:
                            df_temp[col_num] = pd.to_numeric(df_temp[col_num], errors='coerce')

                    # Recalcula Valor Total caso a coluna esteja vazia ou ausente
                    # Fórmula: (Qtd × Vl Unit) + Impostos + Frete  — igual à fórmula do Excel enviado
                    if 'Valor Total' not in df_temp.columns or df_temp['Valor Total'].isna().all():
                        qtd = pd.to_numeric(df_temp.get('Quantidade', 1), errors='coerce').fillna(1)
                        unit = df_temp.get('Valor Unitário', pd.Series(0, index=df_temp.index)).fillna(0)
                        imp  = df_temp.get('Impostos',    pd.Series(0, index=df_temp.index)).fillna(0)
                        frete = df_temp.get('Frete',      pd.Series(0, index=df_temp.index)).fillna(0)
                        df_temp['Valor Total'] = (qtd * unit) + imp + frete

                    if 'Valor Unitário' in df_temp.columns:
                        lista_dfs.append(df_temp)

                if not lista_dfs:
                    st.error("Nenhum dado válido encontrado nos arquivos.")
                    return

                # --- PREPARAÇÃO DOS DADOS ---
                df_geral = pd.concat(lista_dfs, ignore_index=True)
                df_geral['Valor Unitário'] = pd.to_numeric(df_geral['Valor Unitário'], errors='coerce')
                df_geral['Valor Total']    = pd.to_numeric(df_geral['Valor Total'],    errors='coerce')
                index_cols = ['Código', 'Produto', 'Quantidade']

                # Pivot de Preços (comparativo usa Valor Unitário para comparar preço por unidade)
                df_pivot = df_geral.pivot_table(index=index_cols, columns='Fornecedor', values='Valor Unitário', aggfunc='min').reset_index()
                df_pivot.columns = [str(c) for c in df_pivot.columns]
                cols_fornecedores = [c for c in df_pivot.columns if c not in index_cols]

                # Pivot de Valor Total (para calcular o total real do pedido por fornecedor)
                df_total_pivot = df_geral.pivot_table(index=index_cols, columns='Fornecedor', values='Valor Total', aggfunc='min').reset_index()
                df_total_pivot.columns = [str(c) for c in df_total_pivot.columns]

                # Pivot de Prazos
                df_prazo_pivot = pd.DataFrame()
                if tem_prazo and 'Prazo' in df_geral.columns:
                    df_prazo_pivot = df_geral.pivot_table(index=index_cols, columns='Fornecedor', values='Prazo', aggfunc='min').reset_index()
                    df_prazo_pivot.columns = [str(c) for c in df_prazo_pivot.columns]

                # Vencedores por Preço
                df_pivot['Melhor Preço'] = df_pivot[cols_fornecedores].min(axis=1)
                df_pivot['Vencedor_Preco'] = df_pivot[cols_fornecedores].idxmin(axis=1)

                # --- EXIBIÇÃO DO COMPARATIVO ---
                st.write("---")
                st.markdown(f"### Comparativo de Preços ({len(cols_fornecedores)} Fornecedores)")

                def destacar_minimo(row):
                    estilos = ['' for _ in range(len(row))]
                    melhor = row['Melhor Preço']
                    for i, col_nome in enumerate(row.index):
                        if col_nome in cols_fornecedores:
                            if row[col_nome] == melhor and melhor > 0:
                                estilos[i] = 'background-color: #D4EDDA; color: #155724; font-weight: bold'
                    return estilos

                st.dataframe(df_pivot.style.apply(destacar_minimo, axis=1).format(subset=['Melhor Preço'] + cols_fornecedores, precision=2, decimal=',', thousands='.'), use_container_width=True, hide_index=True)

                # --- ESTRATÉGIA DE FECHAMENTO ---
                with st.container(border=True):

                    st.markdown('<h3 style="color: #000000; margin-top: 0;">Estratégia de Fechamento</h3>', unsafe_allow_html=True)
                    col1, col2, col3 = st.columns(3)

                    # Seleção de Estratégia
                    if col1.button("▼ Menor Preço", type="primary" if st.session_state['estrategia_ativa'] == 'menor_preco' else "secondary", use_container_width=True):
                        st.session_state['estrategia_ativa'] = 'menor_preco'; st.rerun()
                    if col2.button("👤 Fornecedor Único", type="primary" if st.session_state['estrategia_ativa'] == 'fornecedor_unico' else "secondary", use_container_width=True):
                        st.session_state['estrategia_ativa'] = 'fornecedor_unico'; st.rerun()
                    if col3.button("⏱️ Menor Entrega", type="primary" if st.session_state['estrategia_ativa'] == 'menor_prazo' else "secondary", use_container_width=True):
                        st.session_state['estrategia_ativa'] = 'menor_prazo'; st.rerun()

                    estrategia = st.session_state['estrategia_ativa']

                    if estrategia == 'menor_preco':
                        st.info("💡 **Menor Preço:** Gera pedidos otimizados pelo menor valor unitário por item.")
                        for f in df_pivot['Vencedor_Preco'].dropna().unique():
                            df_f = df_pivot[df_pivot['Vencedor_Preco'] == f][index_cols + ['Melhor Preço']].rename(columns={'Melhor Preço': 'Valor Unitário'}).copy()
                            # Busca o Valor Total correspondente no pivot de totais
                            if f in df_total_pivot.columns:
                                df_f = df_f.merge(df_total_pivot[index_cols + [f]].rename(columns={f: 'Valor Total'}), on=index_cols, how='left')
                            else:
                                qtd = pd.to_numeric(df_f['Quantidade'], errors='coerce').fillna(1)
                                df_f['Valor Total'] = qtd * df_f['Valor Unitário'].fillna(0)
                            pedidos[f] = df_f

                    elif estrategia == 'fornecedor_unico':
                        escolhido = st.selectbox("Selecione o Fornecedor:", cols_fornecedores)
                        if escolhido:
                            st.warning(f"⚠️ **Atenção:** Comprando somente os itens de {escolhido}.")
                            cols_sel = [c for c in index_cols + ['Valor Unitário', 'Valor Total'] if c in df_geral.columns]
                            df_f = df_geral[df_geral['Fornecedor'] == escolhido][cols_sel].dropna(subset=['Valor Unitário']).copy()
                            if 'Valor Total' not in df_f.columns:
                                qtd = pd.to_numeric(df_f['Quantidade'], errors='coerce').fillna(1)
                                df_f['Valor Total'] = qtd * df_f['Valor Unitário'].fillna(0)
                            pedidos[escolhido] = df_f

                    elif estrategia == 'menor_prazo':
                        if df_prazo_pivot.empty:
                            st.error("Coluna de 'Prazo' não encontrada para esta estratégia.")
                        else:
                            st.success("🚀 **Menor Entrega:** Priorizando o fornecedor mais rápido para cada item.")
                            df_prazo_pivot['Vencedor_Prazo'] = df_prazo_pivot[cols_fornecedores].idxmin(axis=1)
                            for f in cols_fornecedores:
                                itens_f = df_prazo_pivot[df_prazo_pivot['Vencedor_Prazo'] == f]
                                if not itens_f.empty:
                                    df_f = df_pivot.merge(itens_f[index_cols], on=index_cols)[index_cols + [f]].rename(columns={f: 'Valor Unitário'}).copy()
                                    if f in df_total_pivot.columns:
                                        df_f = df_f.merge(df_total_pivot[index_cols + [f]].rename(columns={f: 'Valor Total'}), on=index_cols, how='left')
                                    else:
                                        qtd = pd.to_numeric(df_f['Quantidade'], errors='coerce').fillna(1)
                                        df_f['Valor Total'] = qtd * df_f['Valor Unitário'].fillna(0)
                                    pedidos[f] = df_f

                # --- RENDERIZAÇÃO DOS PEDIDOS E ENVIO ---
                if pedidos:
                    st.write("---")
                    st.markdown('<h3 style="color: #00008b;">Pedidos por Fornecedor</h3>', unsafe_allow_html=True)

                    for fornecedor, df_pedido in pedidos.items():
                        # Usa o Valor Total da planilha (já inclui Qtd × Vl Unit + Impostos + Frete)
                        # Garante que a coluna existe antes de somar
                        if 'Valor Total' not in df_pedido.columns or df_pedido['Valor Total'].isna().all():
                            qtd = pd.to_numeric(df_pedido.get('Quantidade', 1), errors='coerce').fillna(1)
                            df_pedido['Valor Total'] = qtd * pd.to_numeric(df_pedido.get('Valor Unitário', 0), errors='coerce').fillna(0)

                        total_f = pd.to_numeric(df_pedido['Valor Total'], errors='coerce').fillna(0).sum()

                        with st.expander(f"{fornecedor} — Total: R$ {total_f:,.2f}", expanded=True):
                            st.dataframe(df_pedido, use_container_width=True, hide_index=True)

                    # Botão de Envio Unificado (Atende a todas as estratégias)
                    st.write("")
                    c_espaco, c_btn = st.columns([2, 1])
                    with c_btn:
                        if st.button("📨 ENVIAR TODOS OS PEDIDOS", type="primary", use_container_width=True):
                            with st.spinner("Enviando e-mails..."):
                                sucessos, erros = 0, []
                                for f_nome, f_df in pedidos.items():
                                    try:
                                        info = db_forn.get(f_nome) or db_forn.get(f_nome.upper())
                                        if not info or not info.get('email'):
                                            erros.append(f"{f_nome} (Sem e-mail cadastrado)")
                                            continue
                                        
                                        # 2. Prepara o arquivo Excel em memória
                                        buffer = io.BytesIO()
                                        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                                            f_df.to_excel(writer, index=False, sheet_name='Pedido')
                                        buffer.seek(0)
                                        
                                        pdf_bytes = self.gerar_pdf_pedido(f_nome, f_df)
                                        
                                        # 3. Define o texto do e-mail
                                        total_email = pd.to_numeric(f_df['Valor Total'], errors='coerce').fillna(0).sum()
                                        texto_email = f"""
                                            <html>
                                                <body>
                                                    <p>Olá,</p>
                                                    <p>Segue em anexo o <b>Pedido de Compra</b> formalizado pela Rede Espafer.</p>
                                                    <p>Fornecedor: {f_nome}<br>Total: R$ {total_email:,.2f}</p>
                                                    <p>Favor confirmar o recebimento.</p>
                                                </body>
                                            </html>
                                            """
                    
                                        # 4. Chama a função com os nomes de variáveis corretos
                                        # 'anexos' espera uma lista de dicionários conforme sua def original
                                        lista_anexos = [{
                                            'nome': f"Pedido_{f_nome}.pdf",
                                            'dados': pdf_bytes
                                        }]
                    
                                        sucesso, erro_msg = self.enviar_email_com_anexo(
                                            destinatario=info['email'], # Pegando o e-mail do dicionário info
                                            assunto="Pedido de Compra - Rede Espafer",
                                            mensagem=texto_email,
                                            anexos=lista_anexos
                                        )
                    
                                        if sucesso: 
                                            sucessos += 1
                                        else: 
                                            erros.append(f"{f_nome} ({erro_msg})")
                                            
                                    except Exception as e:
                                        erros.append(f"{f_nome} ({str(e)})")
                                
                                if sucessos: 
                                    st.success(f"✅ {sucessos} pedido(s) enviado(s)!")
                                if erros: 
                                    for e in erros: 
                                        st.error(f"❌ Erro em: {e}")

            except Exception as e:
                st.error(f"Erro ao processar planilhas: {e}")
                st.exception(e)
            
    # Auxiliar para atualizar os dados
    def aplicar_atualizacao_orcamento(self, df_novo):
        if st.session_state.dados_orcamento is None or st.session_state.dados_orcamento.empty:
            st.warning("Não há orçamento aberto para atualizar.")
            return

        df_novo['Código'] = df_novo['Código'].astype(str)
        novos_precos = df_novo.set_index('Código')['Valor Unitário'].to_dict()
        
        def update_row(row):
            cod = str(row['idproduto'])
            if cod in novos_precos:
                row['Valor Unitário'] = novos_precos[cod]
                row['Valor Total'] = (row['Qtd Compra'] * row['Valor Unitário'])
            return row
            
        st.session_state.dados_orcamento = st.session_state.dados_orcamento.apply(update_row, axis=1)


@st.dialog("Manual de Uso - Sistema de Inteligência", width="large")
def exibir_manual():
    # Estilo para garantir que o texto não herde recuos de código
    st.markdown("""
Esta seção detalha o funcionamento de cada etapa do processo de suprimentos.

---

### 1. Aba: Gerar Cobertura (O Coração da Necessidade)
Esta aba identifica **o quê**, **quanto** e **onde** comprar.
* **Análise de Giro:** O sistema calcula a velocidade de venda de cada item por filial.
* **Sugestão Automática:** Com base no estoque atual e no lead time (tempo de entrega), o sistema sugere a quantidade ideal para evitar rupturas sem gerar excesso financeiro.
* **Filtros Inteligentes:** Você pode filtrar por fornecedor específico, categoria de produto ou curva ABC para focar nos itens de maior impacto.
* **Ação:** Após revisar as sugestões, os itens selecionados são enviados para a próxima etapa de cotação.

### 2. Aba: Gerar Orçamento (A Simulação de Mercado)
Aqui o sistema simula o cenário de negociação com múltiplos fornecedores.
* **Cálculo de Impostos:** Aplica automaticamente as regras de ICMS (incluindo ST), IPI e outras taxas dependendo da origem do fornecedor.
* **Logística:** Permite inserir custos de frete e prazos de pagamento para calcular o desembolso real.
* **Exportação:** Você pode gerar arquivos individuais para cada fornecedor ou uma planilha mestre em **.csv/Excel** para conferência externa.
* **Ação:** Consolida os valores propostos para que possam ser comparados tecnicamente.

### 3. Aba: Inteligência de Compra (A Decisão Final)
A etapa de auditoria e escolha do vencedor da cotação.
* **Comparativo Side-by-Side:** Exibe uma matriz comparativa onde o sistema destaca o "Melhor Preço" e a "Melhor Condição Geral".
* **Custo Total (TCO):** Não olha apenas para o preço do produto; soma impostos, fretes e condições financeiras para mostrar quem é realmente o fornecedor mais barato.
* **Histórico e Tendência:** Permite visualizar se o preço ofertado está acima ou abaixo da última compra realizada.
* **Ação:** Identifica a oportunidade de economia (Saving) e finaliza o processo para geração do pedido de compra.

---
*Dica: Você pode fechar esta janela clicando no 'X' acima ou clicando em qualquer lugar fora desta caixa.*
""")

    # Botão de fechar que altera o estado para 'False'
    if st.button("✅ Sair do Manual e Voltar ao Processo", type="primary", use_container_width=True):
        st.session_state.show_manual = False
        st.rerun()
 
def verificar_login():
    if "logado" not in st.session_state:
        st.session_state.logado = False
        st.session_state.nome_usuario = ""

    if st.session_state.logado:
        return True

    # Inicializa controle de tentativas de força bruta
    if 'tentativas_login' not in st.session_state:
        st.session_state.tentativas_login = 0
    if 'bloqueado_ate' not in st.session_state:
        st.session_state.bloqueado_ate = None

    # Verifica se ainda está no período de bloqueio
    if st.session_state.bloqueado_ate:
        segundos_restantes = (st.session_state.bloqueado_ate - datetime.datetime.now()).total_seconds()
        if segundos_restantes > 0:
            st.error(f"⛔ Muitas tentativas incorretas. Aguarde {int(segundos_restantes)} segundos.")
            return False
        else:
            # Bloqueio expirou — reset
            st.session_state.tentativas_login = 0
            st.session_state.bloqueado_ate = None

    st.markdown("""
        <style>
        div[data-testid="stForm"] {
            background-color: #121212;
            padding: 40px; border-radius: 15px; border: 2px solid #0047AB; color: white;
        }
        div[data-testid="stForm"] label { color: white !important; }
        </style>
    """, unsafe_allow_html=True)

    st.markdown('<h1 style="text-align: center; color: #0047AB;">Rede Espafer - Login</h1>', unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        with st.form("login_form"):
            user_input = st.text_input("Usuário")
            pass_input = st.text_input("Senha", type="password")

            if st.form_submit_button("ENTRAR", use_container_width=True):

                # --- 1. LOGIN DE EMERGÊNCIA via secrets (nunca hardcoded) ---
                # Configure no .streamlit/secrets.toml:
                # [admin_backup]
                # usuario = "admin"
                # senha = "sua_senha_forte_aqui"
                admin_backup = st.secrets.get("admin_backup", {})
                admin_user = admin_backup.get("usuario", "")
                admin_senha = admin_backup.get("senha", "")

                if admin_user and admin_senha and user_input == admin_user and pass_input == admin_senha:
                    st.session_state.logado = True
                    st.session_state.nome_usuario = "Espafer"
                    st.session_state.tentativas_login = 0
                    logger.info("Login de emergência utilizado")
                    st.rerun()

                # --- 2. TENTATIVA VIA BANCO (com hash bcrypt) ---
                try:
                    db = DatabaseManager()
                    dados_usuario = db.validar_usuario(user_input, pass_input)

                    if dados_usuario:
                        st.session_state.logado = True
                        st.session_state.nome_usuario = dados_usuario[1]
                        st.session_state.tentativas_login = 0
                        st.session_state.bloqueado_ate = None
                        st.success(f"Bem-vindo, {dados_usuario[1]}!")
                        logger.info(f"Login bem-sucedido: {user_input}")
                        st.rerun()
                    else:
                        st.session_state.tentativas_login += 1
                        tentativas = st.session_state.tentativas_login
                        logger.warning(f"Tentativa de login inválida para usuário: {user_input} ({tentativas}/5)")

                        if tentativas >= 5:
                            # Bloqueia por 5 minutos após 5 tentativas falhas
                            st.session_state.bloqueado_ate = datetime.datetime.now() + datetime.timedelta(minutes=5)
                            st.error("⛔ Conta bloqueada por 5 minutos após múltiplas tentativas inválidas.")
                        else:
                            restantes = 5 - tentativas
                            st.error(f"Usuário ou senha inválidos. ({restantes} tentativa(s) restante(s))")

                except Exception:
                    logger.error("Erro de conexão durante o login")
                    st.error("Erro de conexão com o sistema. Tente novamente.")

    return False

def logout():
    st.session_state.logado = False
    st.rerun()
        
# --- EXECUÇÃO FINAL ---
if __name__ == "__main__":
    # 1. Primeiro valida o login
    if not verificar_login():
        st.stop()

    app = AppClientePrime()
    app.render_sidebar()  
    
    # 2. Renderização das telas centrais
    if st.session_state.menu_ativo == "Gerar Cobertura":
        app.tela_cobertura()
    elif st.session_state.menu_ativo == "Gerar Orçamento":
        app.tela_orcamento()
    elif st.session_state.menu_ativo == "Inteligência de Compra":
        app.tela_analise_retorno()
        
    # --- CSS PARA FIXAR O BLOCO NO RODAPÉ (SEM BORDAS) ---
    st.sidebar.markdown("""
        <style>
            /* Espaço para o menu não bater no rodapé */
            [data-testid="stSidebarContent"] { padding-bottom: 180px !important; }

            /* Localização do Container no rodapé */
            div.element-container:has(#ancora-rodape-limpo) + div {
                position: absolute !important;
                bottom: 30px !important;
                left: 0 !important;
                right: 0 !important;
                width: 85% !important;
                margin: auto !important;
                background-color: transparent !important; /* Remove fundo */
                border: none !important; /* Remove borda */
                z-index: 9999;
            }
            
            /* Alinhamento vertical do nome com o botão */
            [data-testid="stHorizontalBlock"] {
                align-items: center !important;
            }

            /* Estilo do nome do usuário */
            .user-label {
                color: #FFFFFF;
                font-size: 0,85rem;
                overflow: hidden;
                text-overflow: ellipsis;
                white-space: nowrap;
            }
        </style>
        <div id="ancora-rodape-limpo"></div>
    """, unsafe_allow_html=True)

    # TUDO O QUE ESTIVER NESTE CONTAINER FICARÁ JUNTO NO RODAPÉ
    with st.sidebar.container():
        # Linha do Usuário e Sair
        c_user, c_sair = st.columns([2.2, 1])
        
        # Nome do usuário alinhado
        c_user.markdown(f'<div class="user-label">👤 {st.session_state.nome_usuario}</div>', unsafe_allow_html=True)
        
        # Botão sair menor e discreto
        if c_sair.button("➜]", key="btn_logout_final", use_container_width=True):
            st.session_state.logado = False
            st.rerun()
        
        # Botão do Manual logo abaixo
        if st.button("📖 Manual do Usuário", key="btn_manual_v6_final", use_container_width=True):
            st.session_state.show_manual = True
            st.rerun()

    # Exibição do manual (modal)
    if st.session_state.get('show_manual', False):
        app.exibir_manual_usuario()