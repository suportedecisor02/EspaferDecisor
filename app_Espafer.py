import bcrypt
import streamlit as st
import pandas as pd
import psycopg2
from fpdf import FPDF
import datetime
import time
import logging
import os
import html
from contextlib import contextmanager
from logging.handlers import RotatingFileHandler
import re

# ==================== CONFIGURAÇÃO DE LOGGING COM ROTAÇÃO ====================
# Máximo de 5MB por arquivo, mantém 3 backups — evita crescimento ilimitado
_log_handler_file = RotatingFileHandler('app.log', maxBytes=5*1024*1024, backupCount=3, encoding='utf-8')
_log_handler_file.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
_log_handler_stream = logging.StreamHandler()
_log_handler_stream.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

logging.basicConfig(level=logging.INFO, handlers=[_log_handler_file, _log_handler_stream])
logger = logging.getLogger(__name__)

# 1. Configuração de Página
st.set_page_config(page_title="Espafer", layout="wide")

# ==================== CONSTANTES ====================
MAX_REGISTROS_QUERY = 2000
MAX_INT_POSTGRES = 2147483647
TIMEOUT_BLOQUEIO_LOGIN = 5
MAX_TENTATIVAS_LOGIN = 5

# Status de pedido centralizados — evita erros de digitação espalhados no código
class StatusPedido:
    PENDENTE   = 'Pendente'
    ENVIADO    = 'Enviado'
    CONFIRMADO = 'Confirmado'

class PDFGenerator:
    """Classe base para geração de PDFs com estilo Espafer"""
    
    AZUL_ESCURO = (0, 71, 171)
    AZUL_CLARO = (235, 245, 255)
    CINZA_TEXTO = (50, 50, 50)
    BRANCO = (255, 255, 255)
    
    @staticmethod
    def criar_pdf_base(titulo, subtitulo, fornecedor=None, info_extra=None):
        """Cria instância base do PDF com cabeçalho Espafer"""
        return PDF(titulo=titulo, subtitulo=subtitulo, fornecedor=fornecedor, info_extra=info_extra)
    
    @staticmethod
    def adicionar_cabecalho_tabela(pdf, colunas, larguras):
        """Adiciona cabeçalho de tabela padronizado"""
        pdf.set_font('Arial', 'B', 10)
        pdf.set_fill_color(*PDFGenerator.AZUL_ESCURO)
        pdf.set_text_color(*PDFGenerator.BRANCO)
        pdf.set_draw_color(*PDFGenerator.AZUL_ESCURO)
        
        for col, largura in zip(colunas, larguras):
            pdf.cell(largura, 10, col, 1, 0, 'C', 1)
        pdf.ln()
    
    @staticmethod
    def adicionar_linha_tabela(pdf, valores, larguras, fill=False):
        """Adiciona linha de tabela com alternância de cores"""
        pdf.set_font('Arial', '', 9)
        pdf.set_text_color(*PDFGenerator.CINZA_TEXTO)
        
        if fill:
            pdf.set_fill_color(*PDFGenerator.AZUL_CLARO)
        else:
            pdf.set_fill_color(*PDFGenerator.BRANCO)
        
        for valor, largura in zip(valores, larguras):
            pdf.cell(largura, 9, str(valor), 'B', 0, 'L', True)
        pdf.ln()

class PDF(FPDF):
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

    def _get_connection(self):
        """Cria conexão com o banco de dados."""
        try:
            return psycopg2.connect(
                host=self.creds.get("host"),
                database=self.creds.get("database"),
                user=self.creds.get("user") or self.creds.get("username"),
                password=self.creds.get("password"),
                port=int(self.creds.get("port", 5432)),
                connect_timeout=10  # FIX: evita travar indefinidamente se o banco estiver inacessível
            )
        except psycopg2.OperationalError as e:
            logger.error(f"Erro de conexão com banco de dados: {e}")
            raise
        except Exception as e:
            logger.error(f"Erro inesperado ao conectar: {e}")
            raise

    @contextmanager
    def get_connection(self):
        """Context manager para conexões — garante fechamento mesmo em exceções."""
        conn = self._get_connection()
        try:
            yield conn
        finally:
            conn.close()

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

        query = "SELECT usuario, nome, senha, perfil FROM usuarios_sistema WHERE LOWER(usuario) = LOWER(%s) AND ativo = TRUE"

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

            usuario_bd, nome_bd, senha_bd, perfil_bd = res
            senha_bd  = str(senha_bd).strip()
            perfil_bd = str(perfil_bd).strip().upper() if perfil_bd else "CLIENTE"
            autenticado = False

            if senha_bd.startswith('$2b$') or senha_bd.startswith('$2a$'):
                try:
                    autenticado = bcrypt.checkpw(senha_bytes, senha_bd.encode('utf-8'))
                except Exception:
                    autenticado = False
            else:
                autenticado = (senha_digitada == senha_bd)
                if autenticado:
                    try:
                        novo_hash = bcrypt.hashpw(senha_bytes, bcrypt.gensalt()).decode('utf-8')
                        cursor.execute(
                            "UPDATE usuarios_sistema SET senha = %s WHERE LOWER(usuario) = LOWER(%s)",
                            (novo_hash, usuario_limpo)
                        )
                        conn.commit()
                        logger.info("Senha do usuario migrada para bcrypt com sucesso.")
                    except Exception as e:
                        logger.warning(f"Nao foi possivel migrar a senha: {e}")

            cursor.close()
            return (usuario_bd, nome_bd, perfil_bd) if autenticado else None

        except Exception as e:
            logger.error(f"Erro ao acessar tabela de usuarios: {e}")
            st.error("Erro ao verificar credenciais. Tente novamente.")
            return None
        finally:
            if conn:
                conn.close()

    def buscar_pedidos_cliente(self, cliente_nome=None):
        """Busca todos os pedidos do cliente (Pendente/Enviado/Confirmado)."""
        conn = None
        try:
            conn = self._get_connection()
            base = """
                SELECT pv.id,
                       pv.numero_pedido,
                       pv.fornecedor_nome,
                       pv.status,
                       pv.data_criacao
                FROM pedidos_vendas pv
                WHERE 1=1
            """
            if cliente_nome:
                query = base + " AND LOWER(pv.cliente_nome) = LOWER(%s) ORDER BY pv.data_criacao DESC"
                return pd.read_sql(query, conn, params=(cliente_nome,))
            else:
                query = base + " ORDER BY pv.data_criacao DESC"
                return pd.read_sql(query, conn)
        except Exception as e:
            logger.error(f"Erro ao buscar pedidos do cliente: {e}")
            return pd.DataFrame()
        finally:
            if conn: conn.close()

    def buscar_pedidos_fornecedor(self, nome_fornecedor=None):
        """Busca pedidos para orçamento (Pendente/Enviado). Se nome_fornecedor informado, filtra por ele."""
        conn = None
        try:
            conn = self._get_connection()
            base = """
                SELECT pv.id,
                       pv.numero_pedido,
                       pv.cliente_nome,
                       pv.fornecedor_nome,
                       pv.status,
                       pv.data_criacao
                FROM pedidos_vendas pv
                WHERE pv.status IN ('Pendente', 'Enviado')
            """
            if nome_fornecedor:
                query = base + " AND LOWER(pv.fornecedor_nome) = LOWER(%s) ORDER BY pv.data_criacao DESC"
                return pd.read_sql(query, conn, params=(nome_fornecedor,))
            else:
                query = base + " ORDER BY pv.data_criacao DESC"
                return pd.read_sql(query, conn)
        except Exception as e:
            logger.error(f"Erro ao buscar pedidos: {e}")
            return pd.DataFrame()
        finally:
            if conn: conn.close()

    def buscar_pedidos_confirmados(self, nome_fornecedor=None):
        """Busca pedidos confirmados. Se nome_fornecedor informado, filtra por ele."""
        conn = None
        try:
            conn = self._get_connection()
            base = """
                SELECT pv.id,
                       pv.numero_pedido,
                       pv.cliente_nome,
                       pv.fornecedor_nome,
                       pv.status,
                       pv.data_criacao
                FROM pedidos_vendas pv
                WHERE pv.status = 'Confirmado'
            """
            if nome_fornecedor:
                query = base + " AND LOWER(pv.fornecedor_nome) = LOWER(%s) ORDER BY pv.data_criacao DESC"
                return pd.read_sql(query, conn, params=(nome_fornecedor,))
            else:
                query = base + " ORDER BY pv.data_criacao DESC"
                return pd.read_sql(query, conn)
        except Exception as e:
            logger.error(f"Erro ao buscar pedidos confirmados: {e}")
            return pd.DataFrame()
        finally:
            if conn: conn.close()

    def buscar_itens_pedido(self, pedido_id):
        """Busca os itens de um pedido específico."""
        conn = None
        try:
            conn = self._get_connection()
            query = """
                SELECT id, codigo_produto, nome_produto, quantidade,
                       COALESCE(valor_unitario, 0)  AS valor_unitario,
                       COALESCE(impostos, 0)         AS impostos,
                       COALESCE(frete, '')           AS frete,
                       COALESCE(prazo_entrega, '')   AS prazo_entrega
                FROM pedidos_itens
                WHERE pedido_id = %s
                ORDER BY id
            """
            return pd.read_sql(query, conn, params=(pedido_id,))
        except Exception as e:
            logger.error(f"Erro ao buscar itens do pedido {pedido_id}: {e}")
            return pd.DataFrame()
        finally:
            if conn: conn.close()

    def salvar_resposta_pedido(self, pedido_id, itens_df, observacao=None):
        """Salva valores preenchidos pelo fornecedor e marca pedido como Enviado."""
        conn = None
        try:
            conn = self._get_connection()
            cur = conn.cursor()
            for _, row in itens_df.iterrows():
                cur.execute("""
                    UPDATE pedidos_itens
                       SET valor_unitario = %s,
                           impostos       = %s,
                           frete          = %s,
                           prazo_entrega  = %s
                     WHERE id = %s
                """, (
                    float(row.get('valor_unitario') or 0),
                    float(row.get('impostos')       or 0),
                    str(row.get('frete')            or ''),
                    str(row.get('prazo_entrega')    or ''),
                    int(row['id'])
                ))
            
            # Atualizar observação do pedido
            if observacao:
                cur.execute("""
                    UPDATE pedidos_vendas 
                    SET observacao = %s, status = %s 
                    WHERE id = %s
                """, (observacao, StatusPedido.ENVIADO, pedido_id))
            else:
                cur.execute("UPDATE pedidos_vendas SET status = %s WHERE id = %s", (StatusPedido.ENVIADO, pedido_id))
            
            conn.commit()
            cur.close()
            return True
        except Exception as e:
            logger.error(f"Erro ao salvar resposta pedido {pedido_id}: {e}")
            if conn: conn.rollback()
            return False
        finally:
            if conn: conn.close()

    def criar_pedido(self, cliente_nome, fornecedor_nome, itens_df, idcobertura=None):
        """Cria um pedido novo e insere os itens."""
        conn = None
        try:
            conn = self._get_connection()
            cur = conn.cursor()
    
            # 1. Gerar número do pedido baseado na data (YYYYMMDD + sequencial)
            data_hoje = datetime.datetime.now().strftime('%Y%m%d')
            
            # Buscar o maior número de pedido que começa com a data de hoje
            cur.execute("""
                SELECT COALESCE(MAX(CAST(numero_pedido AS BIGINT)), 0)
                FROM pedidos_vendas 
                WHERE numero_pedido ~ '^[0-9]+$' 
                AND numero_pedido LIKE %s
            """, (f"{data_hoje}%",))
            
            ultimo_numero = cur.fetchone()[0]
            
            # Se não há pedidos hoje, começa com data + 1
            if ultimo_numero == 0:
                proximo_numero = int(data_hoje + '1')
            else:
                # Incrementa o último número
                proximo_numero = ultimo_numero + 1
            
            numero_pedido = str(proximo_numero)
    
            # 2. Buscar o id_fornecedor na tabela fornecedores
            id_fornecedor_bd = None
            try:
                cur.execute(
                    "SELECT id_fornecedor FROM fornecedores WHERE LOWER(TRIM(fornecedor)) = LOWER(TRIM(%s)) LIMIT 1",
                    (fornecedor_nome,)
                )
                row_forn = cur.fetchone()
                if row_forn:
                    id_fornecedor_bd = row_forn[0]
            except Exception as e:
                logger.warning(f"Não foi possível localizar ID para fornecedor {fornecedor_nome}: {e}")
    
            # 3. Verificar se a coluna fornecedor_nome existe (cacheado em session_state)
            if 'db_tem_col_forn_nome' not in st.session_state:
                cur.execute("""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_name = 'pedidos_vendas' AND column_name = 'fornecedor_nome'
                """)
                st.session_state['db_tem_col_forn_nome'] = cur.fetchone() is not None
            tem_col_forn_nome = st.session_state['db_tem_col_forn_nome']
    
            # 4. Inserir o Cabeçalho do Pedido com idcobertura
            if tem_col_forn_nome:
                cur.execute("""
                    INSERT INTO public.pedidos_vendas
                        (numero_pedido, cliente_nome, fornecedor_nome, id_fornecedor, idcobertura, status, data_criacao)
                    VALUES (%s, %s, %s, %s, %s, 'Pendente', CURRENT_TIMESTAMP)
                    RETURNING id
                """, (numero_pedido, cliente_nome, fornecedor_nome, id_fornecedor_bd, idcobertura))
            else:
                cur.execute("""
                    INSERT INTO public.pedidos_vendas
                        (numero_pedido, cliente_nome, id_fornecedor, idcobertura, status, data_criacao)
                    VALUES (%s, %s, %s, %s, 'Pendente', CURRENT_TIMESTAMP)
                    RETURNING id
                """, (numero_pedido, cliente_nome, id_fornecedor_bd, idcobertura))
    
            pedido_id = cur.fetchone()[0]
    
            # 5. Inserir os Itens do Pedido
            for _, row in itens_df.iterrows():
                qtd = row.get('Qtd Compra') or row.get('quantidade') or row.get('Quantidade') or 0
                cod_prod = str(row.get('idproduto', '') or row.get('codigo_produto', '') or '0')
                nome_prod = str(row.get('produto', '') or row.get('nome_produto', '') or 'Produto não identificado')
    
                cur.execute("""
                    INSERT INTO public.pedidos_itens
                        (pedido_id, codigo_produto, nome_produto, quantidade,
                         valor_unitario, impostos, frete, prazo_entrega)
                    VALUES (%s, %s, %s, %s, NULL, NULL, NULL, NULL)
                """, (
                    pedido_id,
                    cod_prod,
                    nome_prod,
                    float(qtd) if str(qtd).replace('.','').isdigit() else 0.0,
                ))
    
            conn.commit()
            cur.close()
            logger.info(f"Sucesso: Pedido {numero_pedido} criado para {fornecedor_nome} com idcobertura={idcobertura}")
            return numero_pedido
    
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Erro fatal ao criar pedido para {fornecedor_nome}: {e}")
            raise
        finally:
            if conn:
                conn.close()

    @st.cache_data(ttl=3600, show_spinner=False)
    def buscar_filiais(_self):
        if not _self.creds: return []
        conn = None
        try:
            conn = _self._get_connection()
            query = "SELECT DISTINCT nome_empresa FROM venda_itens_consolidado WHERE nome_empresa IS NOT NULL ORDER BY 1"
            df = pd.read_sql(query, conn)
            return df['nome_empresa'].tolist()
        except Exception as e:
            logger.error(f"Erro ao buscar filiais: {e}")
            return []
        finally:
            if conn: conn.close()
        
    @st.cache_data(ttl=3600, show_spinner=False)
    def buscar_marcas(_self, filial=None):
        if not _self.creds: return []
        conn = None
        try:
            conn = _self._get_connection()
            query = "SELECT DISTINCT marca FROM cad_produto WHERE marca IS NOT NULL AND marca != ''"
            query += " ORDER BY 1"
            df = pd.read_sql(query, conn)
            return df.iloc[:, 0].tolist() if not df.empty else []
        except Exception as e:
            logger.error(f"Erro ao buscar marcas: {e}")
            return []
        finally:
            if conn: conn.close()

    @st.cache_data(ttl=3600, show_spinner=False)
    def buscar_grupos(_self, filial=None, marca=None):
        if not _self.creds: return []
        conn = None
        try:
            conn = _self._get_connection()
            query = "SELECT DISTINCT nome_grupo FROM cad_produto WHERE nome_grupo IS NOT NULL AND nome_grupo != ''"
            params = []
            if marca and marca != "TODOS":
                query += " AND marca = %s"; params.append(marca)
            query += " ORDER BY 1"
            df = pd.read_sql(query, conn, params=params)
            return df.iloc[:, 0].tolist() if not df.empty else []
        except Exception as e:
            logger.error(f"Erro ao buscar grupos: {e}")
            return []
        finally:
            if conn: conn.close()

    @st.cache_data(ttl=3600, show_spinner=False)
    def buscar_subgrupos(_self, filial=None, marca=None, grupo=None):
        if not _self.creds: return []
        conn = None
        try:
            conn = _self._get_connection()
            query = "SELECT DISTINCT nome_sub_grupo FROM cad_produto WHERE nome_sub_grupo IS NOT NULL"
            params = []
            if marca and marca != "TODOS":
                query += " AND marca = %s"; params.append(marca)
            if grupo and grupo != "TODOS":
                query += " AND nome_grupo = %s"; params.append(grupo)
            query += " ORDER BY 1"
            df = pd.read_sql(query, conn, params=params)
            return df.iloc[:, 0].tolist() if not df.empty else []
        except Exception as e:
            logger.error(f"Erro ao buscar subgrupos: {e}")
            return []
        finally:
            if conn: conn.close()

    @st.cache_data(ttl=3600, show_spinner=False)
    def buscar_subgrupos1(_self, filial=None, marca=None, grupo=None, subgrupo=None):
        if not _self.creds: return []
        conn = None
        try:
            conn = _self._get_connection()
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
            return df.iloc[:, 0].tolist() if not df.empty else []
        except Exception as e:
            logger.error(f"Erro ao buscar subgrupo1: {e}")
            return []
        finally:
            if conn: conn.close()

    @st.cache_data(ttl=3600, show_spinner=False)
    def buscar_produtos(_self, filial=None, marca=None, grupo=None, subgrupo=None, subgrupo1=None):
        if not _self.creds: return []
        conn = None
        try:
            conn = _self._get_connection()
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
            return df.iloc[:, 0].tolist() if not df.empty else []
        except Exception as e:
            logger.error(f"Erro ao buscar produtos: {e}")
            return []
        finally:
            if conn: conn.close()

    def buscar_pedidos_respondidos(self, cliente_nome=None):
        """Busca pedidos com status Enviado agrupados por idcobertura."""
        conn = None
        try:
            conn = self._get_connection()
            query = """
                SELECT 
                    idcobertura as grupo_pedido,
                    COUNT(DISTINCT id) as qtd_fornecedores,
                    MIN(data_criacao) as data_solicitacao,
                    STRING_AGG(DISTINCT fornecedor_nome, ', ') as fornecedores,
                    STRING_AGG(DISTINCT numero_pedido, ', ' ORDER BY numero_pedido) as numeros_pedidos
                FROM pedidos_vendas
                WHERE status = 'Enviado' AND idcobertura IS NOT NULL
            """
            if cliente_nome:
                query += " AND LOWER(cliente_nome) = LOWER(%s)"
            query += " GROUP BY idcobertura ORDER BY MIN(data_criacao) DESC"
            
            if cliente_nome:
                return pd.read_sql(query, conn, params=(cliente_nome,))
            else:
                return pd.read_sql(query, conn)
        except Exception as e:
            logger.error(f"Erro ao buscar pedidos respondidos: {e}")
            return pd.DataFrame()
        finally:
            if conn: conn.close()

    def buscar_detalhes_comparativo(self, idcobertura):
        """Busca detalhes de todos os fornecedores de um idcobertura."""
        conn = None
        try:
            conn = self._get_connection()
            query = """
                SELECT 
                    pv.fornecedor_nome,
                    pi.codigo_produto,
                    pi.nome_produto,
                    pi.quantidade,
                    COALESCE(pi.valor_unitario, 0) as valor_unitario,
                    COALESCE(pi.impostos, 0) as impostos,
                    COALESCE(pi.frete, '') as frete,
                    pi.prazo_entrega
                FROM pedidos_vendas pv
                JOIN pedidos_itens pi ON pv.id = pi.pedido_id
                WHERE pv.idcobertura = %s
                  AND pv.status = 'Enviado'
                ORDER BY pi.codigo_produto, pv.fornecedor_nome
            """
            return pd.read_sql(query, conn, params=(idcobertura,))
        except Exception as e:
            logger.error(f"Erro ao buscar detalhes comparativo: {e}")
            return pd.DataFrame()
        finally:
            if conn: conn.close()
    
    def buscar_notificacoes(self, nome_usuario, perfil):
        """Busca notificações para o usuário baseado no perfil"""
        conn = None
        try:
            conn = self._get_connection()
            
            # Buscar notificações removidas do usuário
            notif_removidas = self.buscar_notificacoes_removidas(nome_usuario)
            
            notificacoes = []
            
            if perfil in ("ADM", "CLIENTE"):
                query = """
                    SELECT numero_pedido, fornecedor_nome, status, data_criacao
                    FROM pedidos_vendas
                    WHERE LOWER(cliente_nome) = LOWER(%s)
                    ORDER BY data_criacao DESC
                    LIMIT 50
                """
                df = pd.read_sql(query, conn, params=(nome_usuario,))
                
                for _, row in df.iterrows():
                    notif_id = f"{row['numero_pedido']}_{row['status']}"
                    if notif_id in notif_removidas:
                        continue
                        
                    _dc = pd.to_datetime(row['data_criacao'])
                    if _dc.tzinfo is not None: _dc = _dc.tz_localize(None)
                    dias = (datetime.datetime.now() - _dc).days
                    
                    if row['status'] == 'Enviado':
                        notificacoes.append({
                            'id': notif_id,
                            'tipo': 'orcamento_enviado',
                            'mensagem': f"Orçamento recebido de {row['fornecedor_nome']} - Pedido #{row['numero_pedido']}",
                            'icone': '📨',
                            'cor': '#0C5460'
                        })
                    elif row['status'] == 'Pendente' and dias >= 7:
                        notificacoes.append({
                            'id': notif_id,
                            'tipo': 'pedido_atrasado',
                            'mensagem': f"Pedido #{row['numero_pedido']} atrasado ({dias} dias) - {row['fornecedor_nome']}",
                            'icone': '⚠️',
                            'cor': '#8B0000'
                        })
                    elif row['status'] == 'Confirmado':
                        notificacoes.append({
                            'id': notif_id,
                            'tipo': 'pedido_confirmado',
                            'mensagem': f"Pedido #{row['numero_pedido']} confirmado - {row['fornecedor_nome']}",
                            'icone': '✅',
                            'cor': '#155724'
                        })
            
            elif perfil == "FORNECEDOR":
                # Buscar notificações de cobrança
                query_cobranca = """
                    SELECT numero_pedido, cliente, mensagem, data_criacao
                    FROM notificacoes_fornecedor
                    WHERE LOWER(fornecedor) = LOWER(%s)
                    AND lida = FALSE
                    ORDER BY data_criacao DESC
                    LIMIT 10
                """
                try:
                    df_cobranca = pd.read_sql(query_cobranca, conn, params=(nome_usuario,))
                    for _, row in df_cobranca.iterrows():
                        notif_id = f"cobranca_{row['numero_pedido']}"
                        if notif_id not in notif_removidas:
                            notificacoes.append({
                                'id': notif_id,
                                'tipo': 'cobranca_pedido',
                                'mensagem': row['mensagem'],
                                'icone': '⚠️',
                                'cor': '#CC5500'
                            })
                except Exception as e:
                    logger.warning(f'Tabela notificacoes_fornecedor inexistente ou sem permissao: {e}')
                
                # Buscar pedidos pendentes
                query = """
                    SELECT numero_pedido, cliente_nome, status, data_criacao
                    FROM pedidos_vendas
                    WHERE LOWER(fornecedor_nome) = LOWER(%s)
                    AND status IN ('Pendente', 'Enviado')
                    ORDER BY data_criacao DESC
                    LIMIT 50
                """
                df = pd.read_sql(query, conn, params=(nome_usuario,))
                
                for _, row in df.iterrows():
                    notif_id = f"{row['numero_pedido']}_{row['status']}"
                    if notif_id in notif_removidas:
                        continue
                        
                    if row['status'] == 'Pendente':
                        notificacoes.append({
                            'id': notif_id,
                            'tipo': 'novo_pedido',
                            'mensagem': f"Novo pedido #{row['numero_pedido']} de {row['cliente_nome']}",
                            'icone': '📦',
                            'cor': '#856404'
                        })
            
            # Priorizar notificações de cobrança
            notificacoes_cobranca = [n for n in notificacoes if n['tipo'] == 'cobranca_pedido']
            notificacoes_outras = [n for n in notificacoes if n['tipo'] != 'cobranca_pedido']
            return (notificacoes_cobranca + notificacoes_outras)[:10]
        except Exception as e:
            logger.error(f"Erro ao buscar notificações: {e}")
            return []
        finally:
            if conn: conn.close()
    
    def buscar_notificacoes_removidas(self, nome_usuario):
        """Busca IDs de notificações removidas pelo usuário"""
        conn = None
        try:
            conn = self._get_connection()
            cur = conn.cursor()
            
            cur.execute("""
                SELECT notificacao_id FROM notificacoes_removidas
                WHERE LOWER(usuario) = LOWER(%s)
            """, (nome_usuario,))
            
            return set(row[0] for row in cur.fetchall())
        except Exception as e:
            logger.error(f"Erro ao buscar notificações removidas: {e}")
            return set()
        finally:
            if conn:
                conn.close()
    
    def criar_notificacao_fornecedor(self, fornecedor_nome, numero_pedido, cliente_nome):
        """Cria uma notificação para o fornecedor sobre pedido atrasado"""
        conn = None
        try:
            conn = self._get_connection()
            cur = conn.cursor()
            
            mensagem = f"Lembrete: Pedido #{numero_pedido} de {cliente_nome} aguardando resposta"
            cur.execute("""
                INSERT INTO notificacoes_fornecedor (fornecedor, numero_pedido, cliente, mensagem)
                VALUES (%s, %s, %s, %s)
            """, (fornecedor_nome, numero_pedido, cliente_nome, mensagem))
            
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Erro ao criar notificação para fornecedor: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()
    
    def remover_notificacao(self, nome_usuario, notificacao_id):
        """Marca uma notificação como removida permanentemente"""
        conn = None
        try:
            conn = self._get_connection()
            cur = conn.cursor()
            
            # Se for notificação de cobrança, marcar como lida
            if notificacao_id.startswith('cobranca_'):
                numero_pedido = notificacao_id.replace('cobranca_', '')
                cur.execute("""
                    UPDATE notificacoes_fornecedor 
                    SET lida = TRUE 
                    WHERE numero_pedido = %s AND LOWER(fornecedor) = LOWER(%s)
                """, (numero_pedido, nome_usuario))
            
            # Adicionar à lista de removidas
            cur.execute("""
                INSERT INTO notificacoes_removidas (usuario, notificacao_id)
                VALUES (%s, %s)
                ON CONFLICT (usuario, notificacao_id) DO NOTHING
            """, (nome_usuario, notificacao_id))
            
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Erro ao remover notificação: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()
    
    def marcar_pedido_visualizado(self, nome_usuario, pedido_id):
        """Marca um pedido como visualizado permanentemente"""
        conn = None
        try:
            conn = self._get_connection()
            cur = conn.cursor()
            
            cur.execute("""
                INSERT INTO pedidos_visualizados (usuario, pedido_id)
                VALUES (%s, %s)
                ON CONFLICT (usuario, pedido_id) DO NOTHING
            """, (nome_usuario, str(pedido_id)))
            
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Erro ao marcar pedido como visualizado: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()
    
    def pedido_foi_visualizado(self, nome_usuario, pedido_id):
        """Verifica se um pedido já foi visualizado pelo usuário"""
        conn = None
        try:
            conn = self._get_connection()
            cur = conn.cursor()
            
            cur.execute("""
                SELECT 1 FROM pedidos_visualizados
                WHERE LOWER(usuario) = LOWER(%s) AND pedido_id = %s
            """, (nome_usuario, str(pedido_id)))
            
            return cur.fetchone() is not None
        except Exception as e:
            logger.error(f"Erro ao verificar visualização de pedido: {e}")
            return False
        finally:
            if conn:
                conn.close()

    @st.cache_data(ttl=1800, show_spinner=False)
    def buscar_fornecedores(_self):
        if not _self.creds: return pd.DataFrame()
        conn = None
        try:
            conn = _self._get_connection()
            query = """
                SELECT
                    u.nome                          AS fornecedor,
                    COALESCE(f.marca, '')           AS marca
                FROM usuarios_sistema u
                LEFT JOIN fornecedores f
                    ON LOWER(TRIM(f.fornecedor)) = LOWER(TRIM(u.nome))
                WHERE u.perfil = 'FORNECEDOR'
                  AND u.ativo  = TRUE
                ORDER BY u.nome
            """
            df = pd.read_sql(query, conn)
            df.columns = [c.lower() for c in df.columns]
            return df
        except Exception as e:
            logger.error(f"Erro ao buscar fornecedores: {e}")
            return pd.DataFrame(columns=['fornecedor', 'marca'])
        finally:
            if conn: conn.close()

    def verificar_produtos_fornecedor(self, fornecedor_nome, lista_produtos):
        """Verifica quais produtos da lista o fornecedor possui (baseado na marca).
        FIX: substituido loop N+1 por uma unica query IN para todos os produtos.
        """
        if not self.creds or not lista_produtos: return set(), set()
        conn = None
        try:
            conn = self._get_connection()
            # Buscar marcas do fornecedor
            query_marcas = """
                SELECT DISTINCT TRIM(UPPER(f.marca)) as marca
                FROM fornecedores f
                WHERE LOWER(TRIM(f.fornecedor)) = LOWER(TRIM(%s))
                  AND f.marca IS NOT NULL AND f.marca != ''
            """
            df_marcas = pd.read_sql(query_marcas, conn, params=(fornecedor_nome,))
            marcas_fornecedor = set(df_marcas['marca'].tolist()) if not df_marcas.empty else set()

            if not marcas_fornecedor:
                return set(), set(lista_produtos)

            # FIX N+1: buscar marcas de TODOS os produtos em uma unica query usando ANY
            ids_str = [str(p).strip() for p in lista_produtos]
            query_prods = """
                SELECT TRIM(CAST(codacessog AS TEXT)) as cod,
                       TRIM(UPPER(marca)) as marca
                FROM cad_produto
                WHERE TRIM(CAST(codacessog AS TEXT)) = ANY(%s)
            """
            df_prods = pd.read_sql(query_prods, conn, params=(ids_str,))
            mapa_marca = dict(zip(df_prods['cod'], df_prods['marca'])) if not df_prods.empty else {}

            possui = set()
            nao_possui = set()
            for prod_id in lista_produtos:
                marca_prod = mapa_marca.get(str(prod_id).strip())
                if marca_prod and marca_prod in marcas_fornecedor:
                    possui.add(prod_id)
                else:
                    nao_possui.add(prod_id)

            return possui, nao_possui
        except Exception as e:
            logger.error(f"Erro ao verificar produtos do fornecedor: {e}")
            return set(), set(lista_produtos)
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
                    COALESCE(f.fornecedor, f2.fornecedor) as fornecedor,
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
                LEFT JOIN fornecedores f2 ON (c.marca IS NULL OR TRIM(c.marca) = '') AND TRIM(UPPER(f2.marca)) = ''
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

    def inicializar_estado(self):
        perfil = st.session_state.get('perfil_usuario', 'CLIENTE')
        menu_default = "Pedidos" if perfil == "FORNECEDOR" else "Gerar Cobertura"
        
        # Inicialização consolidada de estados
        estados_default = {
            'menu_ativo': menu_default,
            'modo_analise_atual': "COMPRA",
            'tentativas_login': 0,
            'perfil_usuario': "CLIENTE",
            'db_fornecedores': {},
            'itens_removidos': [],
            'dados_orcamento': None,
            'df_analise_cache': pd.DataFrame(),
            'filtros_anteriores': None,
            'filtrar_sem_fornecedor': False,
            'cache_qtd_itens': {},
            'pedidos_com_alteracao': set()
        }
        
        for key, default_value in estados_default.items():
            if key not in st.session_state:
                st.session_state[key] = default_value

    def aplicar_estilos(self):
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
                    transition: transform 0.2s ease-in-out !important;
                }
                div.stButton > button:hover {
                    transform: scale(1.02) !important;
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
                    background-color: #F0F2F6 !important;
                    color: #000000 !important;
                    border-color: #0047AB !important;
                    transform: scale(1.02) !important;
                }
                    
                .pedido-container-row {
                border: 1px solid #E0E0E0;
                border-radius: 10px;
                padding: 15px;
                margin-bottom: 12px;
                background-color: #FFFFFF;
                transition: transform 0.2s, box-shadow 0.2s;
            }
        
            .pedido-container-row:hover {
                border-color: #0047AB;
                box-shadow: 0 4px 8px rgba(0,0,0,0.05);
            }
                    
            </style>
        """, unsafe_allow_html=True)

    def _marcar_pedido_visualizado(self, pedido_id):
        """Marca pedido como visualizado permanentemente no banco e remove notificação"""
        nome_usuario = st.session_state.get('nome_usuario', '')
        self.db.marcar_pedido_visualizado(nome_usuario, pedido_id)
        
        # Remover notificação correspondente ao pedido
        # Buscar pedido para obter número e status
        try:
            with self.db.get_connection() as conn:
                cur = conn.cursor()
                cur.execute("""
                    SELECT numero_pedido, status FROM pedidos_vendas WHERE id = %s
                """, (pedido_id,))
                result = cur.fetchone()
                if result:
                    numero_pedido, status = result
                    notif_id = f"{numero_pedido}_{status}"
                    self.db.remover_notificacao(nome_usuario, notif_id)
        except Exception as e:
            logger.error(f"Erro ao remover notificação do pedido {pedido_id}: {e}")
    
    def _pedido_eh_novo(self, pedido_id):
        """Verifica se pedido é novo (não visualizado) consultando banco"""
        nome_usuario = st.session_state.get('nome_usuario', '')
        return not self.db.pedido_foi_visualizado(nome_usuario, pedido_id)
    
    def _pedido_eh_urgente(self, pedido_id, fornecedor_nome):
        """Verifica se pedido foi notificado pelo cliente (urgente) - independente de lida"""
        try:
            with self.db.get_connection() as conn:
                cur = conn.cursor()
                cur.execute("""
                    SELECT pv.numero_pedido
                    FROM pedidos_vendas pv
                    JOIN notificacoes_fornecedor nf ON pv.numero_pedido = nf.numero_pedido
                    WHERE pv.id = %s 
                    AND LOWER(nf.fornecedor) = LOWER(%s)
                """, (pedido_id, fornecedor_nome))
                return cur.fetchone() is not None
        except Exception as e:
            logger.error(f"Erro ao verificar urgencia do pedido {pedido_id}: {e}")
            return False

    def render_sidebar(self):
        perfil = st.session_state.get('perfil_usuario', 'CLIENTE')
        with st.sidebar:
            # Logo e Usuário no topo
            logo_path = os.path.join(os.path.dirname(__file__), 'imagens', 'LOGO-ESPAFER.png')
            if os.path.exists(logo_path):
                st.image(logo_path, use_container_width=True)
            else:
                st.markdown(
                    '<div style="padding:10px 0px;">' 
                    '<h1 style="color:#0047AB !important; font-weight:900; margin-bottom: 8px; text-align: center;">ESPAFER</h1>',
                    unsafe_allow_html=True
                )
            
            # Card Único Profissional com Seta de Logout
            perfil_texto = {"ADM": "🔑 Administrador", "CLIENTE": "🏪 Cliente", "FORNECEDOR": "🚚 Fornecedor"}.get(perfil, "👤 Usuário")
            # FIX: html.escape() evita XSS caso nome do usuário contenha caracteres HTML
            nome_usuario_safe = html.escape(str(st.session_state.nome_usuario))
            
            st.markdown(f"""
                <style>
                .user-box {{
                    background: linear-gradient(135deg, #0a0a0a 0%, #1a1a1a 100%);
                    border-radius: 12px;
                    padding: 16px 18px;
                    margin-bottom: 20px;
                    border-left: 4px solid #0047AB;
                    box-shadow: 0 4px 16px rgba(0,71,171,0.2);
                    display: flex;
                    align-items: center;
                    justify-content: space-between;
                }}
                .user-details {{
                    flex: 1;
                }}
                .user-name-text {{
                    color: #FFFFFF !important;
                    font-size: 1.1rem;
                    font-weight: 800;
                    margin: 0 0 6px 0;
                    letter-spacing: 0.5px;
                }}
                .user-role-text {{
                    color: #7B8394 !important;
                    font-size: 0.75rem;
                    font-weight: 600;
                    margin: 0;
                    text-transform: uppercase;
                    letter-spacing: 1px;
                }}
                .logout-arrow {{
                    color: #FFFFFF !important;
                    font-size: 1.5rem;
                    cursor: pointer;
                    opacity: 0.6;
                    transition: all 0.2s ease;
                    padding: 6px 10px;
                    border-radius: 6px;
                }}
                .logout-arrow:hover {{
                    opacity: 1;
                    background: rgba(0,71,171,0.2);
                    transform: translateX(-3px);
                }}
                </style>
                <form method="get" action="?logout=true">
                    <div class="user-box">
                        <div class="user-details">
                            <p class="user-role-text">{perfil_texto}</p>
                            <p class="user-name-text">{nome_usuario_safe}</p>
                        </div>
                        <button type="submit" class="logout-arrow" style="background:none;border:none;">↩</button>
                    </div>
                </form>
            """, unsafe_allow_html=True)
            
            # Detectar logout via query params
            if st.query_params.get("logout") == "true":
                st.session_state.logado = False
                st.query_params.clear()
                st.rerun()
            
            st.markdown("---")

            # ── MENUS POR PERFIL ──────────────────────────────────────
            if perfil in ("ADM", "CLIENTE"):
                with st.expander("PEDIDO DE COMPRA", expanded=True):
                    opcoes = ["Gerar Cobertura", "Meus Pedidos", "Inteligência de Compra"]
                    for opt in opcoes:
                        tipo = "primary" if st.session_state.menu_ativo == opt else "secondary"
                        if st.button(opt, key=f"sub_{opt}", type=tipo, use_container_width=True):
                            # Limpar caches ao trocar de menu
                            if st.session_state.menu_ativo != opt:
                                keys_to_clear = [k for k in st.session_state.keys() if k.startswith('lista_') or k.startswith('cache_') or k.startswith('detalhes_')]
                                for key in keys_to_clear:
                                    del st.session_state[key]
                                if 'pedido_aberto' in st.session_state:
                                    del st.session_state['pedido_aberto']
                                if 'pedido_confirmado_aberto' in st.session_state:
                                    del st.session_state['pedido_confirmado_aberto']
                                if 'solicitacao_aberta' in st.session_state:
                                    del st.session_state['solicitacao_aberta']
                            st.session_state.menu_ativo = opt
                            st.rerun()

            if perfil in ("ADM", "FORNECEDOR"):
                with st.expander("FORNECEDOR", expanded=(perfil == "FORNECEDOR")):
                    opcoes_forn = ["Orçamento", "Pedidos"]
                    for opt in opcoes_forn:
                        tipo = "primary" if st.session_state.menu_ativo == opt else "secondary"
                        if st.button(opt, key=f"sub_{opt}", type=tipo, use_container_width=True):
                            # Limpar caches ao trocar de menu
                            if st.session_state.menu_ativo != opt:
                                keys_to_clear = [k for k in st.session_state.keys() if k.startswith('lista_') or k.startswith('cache_')]
                                for key in keys_to_clear:
                                    del st.session_state[key]
                                if 'pedido_aberto' in st.session_state:
                                    del st.session_state['pedido_aberto']
                                if 'pedido_confirmado_aberto' in st.session_state:
                                    del st.session_state['pedido_confirmado_aberto']
                            st.session_state.menu_ativo = opt
                            st.rerun()

            # --- FILTROS DINÂMICOS (só aparece para Cliente/ADM com dados) ---
            if perfil in ("ADM", "CLIENTE") and "dados_completos" in st.session_state:
                df_ref = st.session_state.dados_completos
                st.markdown("---")
                st.markdown("### 🔍 Filtros")
                opcoes_marcas = sorted(df_ref['marca'].dropna().unique())
                marcas = st.multiselect("Marcas:", opcoes_marcas, key="f_marca")
                if marcas:
                    df_sub = df_ref[df_ref['marca'].isin(marcas)]
                    opcoes_sub = sorted(df_sub['nome_sub_grupo'].dropna().unique())
                else:
                    opcoes_sub = sorted(df_ref['nome_sub_grupo'].dropna().unique())
                subs = st.multiselect("Subcategorias:", opcoes_sub, key="f_sub")
                st.session_state['filtro_marca'] = marcas
                st.session_state['filtro_sub'] = subs

    def salvar_edicoes(self):
        if "editor_orcamento_grid" in st.session_state:
            state = st.session_state["editor_orcamento_grid"]
            edited_rows = state.get("edited_rows", {})
            
            # Garantir que df_view_atual existe e está sincronizado
            if 'dados_orcamento' not in st.session_state or st.session_state.dados_orcamento is None:
                return
            
            df_exibido = st.session_state.get('df_view_atual')
            
            if df_exibido is not None and not df_exibido.empty:
                for idx_visual, changes in edited_rows.items():
                    try:
                        idx_int = int(idx_visual)
                        if idx_int >= len(df_exibido):
                            continue
                        id_produto = df_exibido.loc[idx_int, 'idproduto']
                        
                        mask = st.session_state.dados_orcamento['idproduto'] == id_produto
                        
                        for col, value in changes.items():
                            st.session_state.dados_orcamento.loc[mask, col] = value
                    except Exception as e:
                        logger.error(f"Erro ao salvar edição: {e}")

    def gerar_pdf_cotacao(self, fornecedor, grupo_itens):
        pdf = PDFGenerator.criar_pdf_base(fornecedor=fornecedor)
        pdf.add_page()
        
        # Cabeçalho da tabela
        PDFGenerator.adicionar_cabecalho_tabela(pdf, ['CÓDIGO', '  DESCRIÇÃO DO PRODUTO', 'QTD'], [30, 130, 30])
        
        # Corpo da tabela
        fill = False
        for _, row in grupo_itens.iterrows():
            produto_nome = str(row['produto']).upper()[:70]
            produto_nome = produto_nome.encode('latin-1', 'replace').decode('latin-1')
            
            valores = [
                str(row['idproduto']),
                '  ' + produto_nome,
                f"{row['Qtd Compra']:.0f}  "
            ]
            PDFGenerator.adicionar_linha_tabela(pdf, valores, [30, 130, 30], fill)
            fill = not fill
        
        # Rodapé
        pdf.ln(5)
        pdf.set_font('Arial', 'I', 8)
        pdf.set_text_color(100, 100, 100)
        data_atual = datetime.datetime.now().strftime('%d/%m/%Y %H:%M')
        pdf.cell(190, 10, f'Documento gerado automaticamente pelo Sistema Rede Espafer em {data_atual}', 0, 0, 'R')

        return pdf.output(dest='S').encode('latin-1', 'replace')

    def gerar_pdf_sobra(self, df, dias_corte, dias_alvo):
        # Verificar se as colunas já foram renomeadas
        if 'estoque' in df.columns:
            col_estoque = 'estoque'
        else:
            col_estoque = 'vol_estoque'
        
        if f'venda({dias_alvo}D)' in df.columns:
            col_venda = f'venda({dias_alvo}D)'
        else:
            col_venda = 'venda_periodo'
        
        df = df.sort_values(by=col_estoque, ascending=False)
        msg_filtro = f"Filtro aplicado: Itens com cobertura superior a {dias_corte} dias."

        pdf = PDF(
            titulo="RELATÓRIO DE SOBRA DE ESTOQUE", 
            subtitulo="Ordenado por Volume de Estoque (Decrescente)", 
            fornecedor=None,
            info_extra=msg_filtro
        )
        pdf.add_page()

        AZUL_ESCURO = (0, 71, 171)
        AZUL_CLARO = (235, 245, 255)
        CINZA_TEXTO = (40, 40, 40)
        BRANCO = (255, 255, 255)

        pdf.set_font('Arial', 'B', 8)
        pdf.set_fill_color(*AZUL_ESCURO)
        pdf.set_text_color(*BRANCO)
        pdf.set_draw_color(*AZUL_ESCURO)

        pdf.cell(12, 8, 'LOJA', 1, 0, 'C', 1)
        pdf.cell(20, 8, 'CODIGO', 1, 0, 'C', 1)
        pdf.cell(95, 8, 'PRODUTO', 1, 0, 'C', 1)
        pdf.cell(30, 8, 'ESTOQUE', 1, 0, 'C', 1)
        pdf.cell(33, 8, f'VENDA ({dias_alvo}D)', 1, 1, 'C', 1)

        pdf.set_font('Arial', '', 7)
        pdf.set_text_color(*CINZA_TEXTO)

        fill = False 
        for _, row in df.iterrows():
            if fill:
                pdf.set_fill_color(*AZUL_CLARO)
            else:
                pdf.set_fill_color(*BRANCO)

            produto_nome = str(row['produto'])[:50]
            produto_nome = produto_nome.encode('latin-1', 'replace').decode('latin-1')

            pdf.cell(12, 7, str(row['filial'])[:5], 'B', 0, 'C', True)
            pdf.cell(20, 7, str(row['idproduto'])[:10], 'B', 0, 'C', True)
            pdf.cell(95, 7, ' ' + produto_nome, 'B', 0, 'L', True)
            
            pdf.set_font('Arial', 'B', 7)
            pdf.cell(30, 7, f"{row[col_estoque]:.0f} und", 'B', 0, 'R', True)
            
            pdf.set_font('Arial', '', 7)
            pdf.cell(33, 7, f"{row[col_venda]:.1f}", 'B', 1, 'R', True)

            fill = not fill 

        pdf.ln(3)
        pdf.set_font('Arial', 'I', 7)
        pdf.set_text_color(120, 120, 120)
        data_atual = datetime.datetime.now().strftime('%d/%m/%Y %H:%M')
        pdf.cell(190, 6, f'Rede Espafer - Gerado em: {data_atual}', 0, 0, 'R')

        return pdf.output(dest='S').encode('latin-1', 'replace')
    
    def gerar_pdf_pedido(self, fornecedor, df_pedido):
        pdf = FPDF()
        pdf.add_page()
        
        AZUL = (0, 71, 171)  # #0047AB
        BRANCO = (255, 255, 255)
        PRETO = (40, 40, 40)
        CINZA = (100, 100, 100)
        
        # Cabeçalho profissional
        pdf.set_font('Arial', 'B', 18)
        pdf.set_text_color(*PRETO)
        pdf.cell(0, 10, 'REDE ESPAFER', 0, 1, 'L')
        pdf.set_font('Arial', '', 9)
        pdf.set_text_color(*CINZA)
        pdf.cell(0, 5, 'Solicitacao de Cotacao de Materiais', 0, 1, 'L')
        pdf.ln(8)
        
        # Informações do pedido
        pdf.set_font('Arial', 'B', 10)
        pdf.set_text_color(*PRETO)
        pdf.cell(0, 6, f'Destinatario: {fornecedor}', 0, 1, 'L')
        pdf.set_font('Arial', '', 9)
        pdf.set_text_color(*CINZA)
        pdf.cell(0, 5, f'Emitido em: {datetime.datetime.now().strftime("%d/%m/%Y as %H:%M")}', 0, 1, 'L')
        pdf.cell(0, 5, 'Favor retornar com valores e prazos de entrega', 0, 1, 'L')
        pdf.ln(8)
        
        # Ordena por quantidade (maior primeiro)
        df_sorted = df_pedido.copy()
        df_sorted['qtd_sort'] = df_sorted.apply(
            lambda row: int(row.get('quantidade', row.get('Quantidade', 0))), axis=1
        )
        df_sorted = df_sorted.sort_values('qtd_sort', ascending=False)
        
        # Verifica se tem coluna de valor unitário
        tem_valor = 'Valor Unitário' in df_sorted.columns
        
        # Tabela - Cabeçalho AZUL
        pdf.set_font('Arial', 'B', 9)
        pdf.set_fill_color(*AZUL)
        pdf.set_text_color(*BRANCO)
        pdf.set_draw_color(0, 0, 0)
        
        if tem_valor:
            pdf.cell(30, 10, 'CODIGO', 1, 0, 'C', True)
            pdf.cell(105, 10, 'PRODUTO', 1, 0, 'C', True)
            pdf.cell(25, 10, 'QTD', 1, 0, 'C', True)
            pdf.cell(30, 10, 'VL. UNIT.', 1, 1, 'C', True)
        else:
            pdf.cell(35, 10, 'CODIGO', 1, 0, 'C', True)
            pdf.cell(130, 10, 'PRODUTO', 1, 0, 'C', True)
            pdf.cell(25, 10, 'QTD', 1, 1, 'C', True)
        
        # Corpo da tabela
        pdf.set_font('Arial', '', 8)
        pdf.set_text_color(*PRETO)
        pdf.set_fill_color(*BRANCO)
        
        for _, row in df_sorted.iterrows():
            codigo = str(row.get('codigo_produto', row.get('Codigo', '')))
            produto = str(row.get('nome_produto', row.get('Produto', '')))[:60]
            qtd = int(row.get('quantidade', row.get('Quantidade', 0)))
            
            if tem_valor:
                valor_unit = float(row.get('Valor Unitário', 0))
                pdf.cell(30, 8, codigo, 1, 0, 'C')
                pdf.cell(105, 8, '  ' + produto, 1, 0, 'L')
                pdf.set_font('Arial', 'B', 8)
                pdf.cell(25, 8, str(qtd), 1, 0, 'C')
                pdf.cell(30, 8, f'R$ {valor_unit:.2f}', 1, 1, 'R')
                pdf.set_font('Arial', '', 8)
            else:
                pdf.cell(35, 8, codigo, 1, 0, 'C')
                pdf.cell(130, 8, '  ' + produto, 1, 0, 'L')
                pdf.set_font('Arial', 'B', 8)
                pdf.cell(25, 8, str(qtd), 1, 1, 'C')
                pdf.set_font('Arial', '', 8)
        
        # Total se houver valor unitário
        if tem_valor:
            pdf.ln(5)
            total = (df_sorted['quantidade'] * df_sorted['Valor Unitário']).sum()
            pdf.set_font('Arial', 'B', 10)
            pdf.set_text_color(*AZUL)
            pdf.cell(0, 8, f'TOTAL: R$ {total:,.2f}', 0, 1, 'R')
        
        # Rodapé
        pdf.ln(10)
        pdf.set_font('Arial', '', 8)
        pdf.set_text_color(120, 120, 120)
        pdf.cell(0, 5, 'Rede Espafer - Departamento de Compras', 0, 0, 'C')
        
        return pdf.output(dest='S').encode('latin-1')

    def tela_visualizar_pedidos_cliente(self):
        """Tela para cliente visualizar seus pedidos com alertas de prazo"""
        cliente_nome = st.session_state.get('nome_usuario', '')
        
        st.markdown('<h1 style="color:black; font-weight:900;">📋 Meus Pedidos</h1>', unsafe_allow_html=True)
        
        # CSS para status
        st.markdown("""
            <style>
            .black-text { color: #333333 !important; font-weight: 500; }
            .pedido-num-bold { color: #0047AB !important; font-weight: 900; }
            .header-title { color: #000000 !important; font-weight: 800; font-size: 1rem; }
            /* Botão de download PDF com mesmo estilo dos botões normais */
            .stDownloadButton button {
                background: linear-gradient(135deg, #0047AB 0%, #000000 150%) !important;
                color: white !important;
                border: none !important;
                font-weight: 700 !important;
                transition: transform 0.2s ease-in-out !important;
            }
            .stDownloadButton button:hover {
                background: linear-gradient(135deg, #0047AB 0%, #000000 150%) !important;
                color: white !important;
                transform: scale(1.02) !important;
            }
            </style>
        """, unsafe_allow_html=True)
        
        # Verificar se há pedido aberto
        pedido_aberto = st.session_state.get('pedido_cliente_aberto')
        
        if pedido_aberto:
            # MODO DETALHE - Mostrar apenas o pedido aberto
            cache_key_lista = f'lista_pedidos_cliente_{cliente_nome}'
            if cache_key_lista in st.session_state:
                df_pedidos = st.session_state[cache_key_lista]
            else:
                df_pedidos = self.db.buscar_pedidos_cliente(cliente_nome)
            if not df_pedidos.empty:
                row_pedido = df_pedidos[df_pedidos['id'] == pedido_aberto].iloc[0]
                num = str(row_pedido['numero_pedido'])
                fornecedor = str(row_pedido['fornecedor_nome'])
                status = str(row_pedido['status'])
                
                st.markdown(f'<h2 style="color:black; font-weight:900;">Detalhes do Pedido #{num}</h2>', unsafe_allow_html=True)
                st.info(f"📦 Fornecedor: **{fornecedor}** | Status: **{status}**")
                
                df_itens = self.db.buscar_itens_pedido(pedido_aberto)
                if not df_itens.empty:
                    st.dataframe(df_itens[['codigo_produto', 'nome_produto', 'quantidade']], use_container_width=True, hide_index=True)
                
                col_voltar, col_pdf = st.columns(2)
                with col_voltar:
                    if st.button("← Voltar para Lista", type="secondary", use_container_width=True):
                        del st.session_state['pedido_cliente_aberto']
                        cache_key_lista = f'lista_pedidos_cliente_{cliente_nome}'
                        if cache_key_lista in st.session_state:
                            del st.session_state[cache_key_lista]
                        st.rerun()
                with col_pdf:
                    pdf_bytes = self.gerar_pdf_pedido(fornecedor, df_itens)
                    st.download_button("📄 Baixar PDF", pdf_bytes, f"Pedido_{num}.pdf", "application/pdf", use_container_width=True, type="primary")
            return
        
        # MODO LISTA - Mostrar todos os pedidos
        cache_key_lista = f'lista_pedidos_cliente_{cliente_nome}'
        if cache_key_lista not in st.session_state:
            st.session_state[cache_key_lista] = self.db.buscar_pedidos_cliente(cliente_nome)
        df_pedidos = st.session_state[cache_key_lista]
        
        if df_pedidos.empty:
            st.info("Nenhum pedido encontrado.")
            return
        
        # Legenda de cores
        st.info("INFO: 🔵 **Azul:** Orçamento enviado | 🟢 **Verde:** Pedido confirmado | 🟡 **Amarelo:** Pedido recente (até 2 dias) | 🟠 **Laranja:** Atenção necessária (3-6 dias) | 🔴 **Vermelho:** Urgente (7+ dias)")
        
        with st.container(border=True):
            c_num, c_forn, c_status, c_data, c_acao = st.columns([1.5, 2.5, 1.5, 1.8, 1.5])
            c_num.markdown('<div class="header-title">Pedido</div>', unsafe_allow_html=True)
            c_forn.markdown('<div class="header-title">Fornecedor</div>', unsafe_allow_html=True)
            c_status.markdown('<div class="header-title" style="text-align: center;">Status</div>', unsafe_allow_html=True)
            c_data.markdown('<div class="header-title" style="text-align: center;">Data</div>', unsafe_allow_html=True)
            c_acao.markdown('<div class="header-title" style="text-align: center;">Ações</div>', unsafe_allow_html=True)
            st.markdown("<div style='margin-bottom: 10px;'></div>", unsafe_allow_html=True)
            
            for _, row in df_pedidos.iterrows():
                pedido_id = int(row['id'])
                num = str(row['numero_pedido'])
                fornecedor = str(row['fornecedor_nome'])
                status = str(row['status'])
                data_criacao = pd.to_datetime(row['data_criacao'])
                # FIX: normalizar timezone — evita TypeError ao subtrair datetime naive de tz-aware
                if data_criacao.tzinfo is not None:
                    data_criacao = data_criacao.tz_localize(None)
                data_str = data_criacao.strftime('%d/%m/%Y')
                
                # Calcular dias desde criação
                dias_desde_criacao = (datetime.datetime.now() - data_criacao).days
                
                # Badge de status com cores dinâmicas para Pendente
                if status == "Confirmado":
                    badge_bg, badge_txt = "#D4EDDA", "#155724"  # Verde
                elif status == "Enviado":
                    badge_bg, badge_txt = "#D1ECF1", "#0C5460"  # Azul
                else:  # Pendente
                    if dias_desde_criacao >= 7:
                        badge_bg, badge_txt = "#FFCCCC", "#8B0000"  # Vermelho
                    elif dias_desde_criacao >= 3:
                        badge_bg, badge_txt = "#FFE5CC", "#CC5500"  # Laranja
                    else:
                        badge_bg, badge_txt = "#FFF3CD", "#856404"  # Amarelo
                
                pedido_aberto = st.session_state.get('pedido_cliente_aberto')
                esta_aberto = (pedido_aberto == pedido_id)
                estilo_pedido = 'background-color: #E3F2FD; border-left: 4px solid #0047AB; padding: 8px; border-radius: 5px;' if esta_aberto else 'padding-top:8px;'
                
                col_n, col_f, col_s, col_d, col_a = st.columns([1.5, 2.5, 1.5, 1.8, 1.5])
                
                col_n.markdown(f'<div style="{estilo_pedido}"><div class="pedido-num-bold">#{num}</div></div>', unsafe_allow_html=True)
                col_f.markdown(f'<div class="black-text" style="padding-top:8px;">{fornecedor}</div>', unsafe_allow_html=True)
                col_s.markdown(f'''
                    <div style="background:{badge_bg}; color:{badge_txt}; padding:4px 8px; 
                    border-radius:12px; font-size:0.8rem; font-weight:bold; text-align:center; margin-top:8px;">
                        {status}
                    </div>
                ''', unsafe_allow_html=True)
                col_d.markdown(f'<div class="black-text" style="padding-top:8px; text-align:center;">{data_str}</div>', unsafe_allow_html=True)
                
                # Botões de ação baseados no status e dias
                if status == "Pendente" and dias_desde_criacao >= 3:
                    # Mostrar três botões: Visualizar, Notificar e PDF
                    col_a1, col_a2, col_a3 = col_a.columns(3)
                    texto_botao = "✖" if esta_aberto else "Visualizar"
                    if col_a1.button(texto_botao, key=f"ver_cli_{pedido_id}", use_container_width=True):
                        if pedido_aberto == pedido_id:
                            del st.session_state['pedido_cliente_aberto']
                            if cache_key_lista in st.session_state:
                                del st.session_state[cache_key_lista]
                        else:
                            st.session_state['pedido_cliente_aberto'] = pedido_id
                        st.rerun()
                    if col_a2.button("Notificar", key=f"alert_{pedido_id}", use_container_width=True):
                        if self.db.criar_notificacao_fornecedor(fornecedor, num, cliente_nome):
                            st.toast(f"🔔 Notificação enviada para {fornecedor}!", icon="✅")
                        else:
                            st.toast(f"❌ Erro ao enviar notificação", icon="⚠️")
                    df_itens_pdf = self.db.buscar_itens_pedido(pedido_id)
                    pdf_bytes = self.gerar_pdf_pedido(fornecedor, df_itens_pdf)
                    if col_a3.button("PDF", key=f"pdf_cli_{pedido_id}", use_container_width=True):
                        pass  # Download button não precisa de ação
                    col_a3.download_button("PDF", pdf_bytes, f"Pedido_{num}.pdf", "application/pdf", key=f"pdf_dl_{pedido_id}", use_container_width=True)
                else:
                    # Mostrar dois botões: Visualizar e PDF
                    col_a1, col_a2 = col_a.columns(2)
                    texto_botao = "✖ Fechar" if esta_aberto else "Visualizar"
                    if col_a1.button(texto_botao, key=f"ver_cli_{pedido_id}", use_container_width=True):
                        st.session_state['pedido_cliente_aberto'] = pedido_id
                        st.rerun()
                    df_itens_pdf = self.db.buscar_itens_pedido(pedido_id)
                    pdf_bytes = self.gerar_pdf_pedido(fornecedor, df_itens_pdf)
                    col_a2.download_button("PDF", pdf_bytes, f"Pedido_{num}.pdf", "application/pdf", key=f"pdf_cli_{pedido_id}", use_container_width=True)
        
        # Fim da lista

    def tela_orcamento(self):
        st.markdown('<h1 style="color:black; font-weight:900;">Gerar Orçamento</h1>', unsafe_allow_html=True)

        if st.session_state.dados_orcamento is None or st.session_state.dados_orcamento.empty:
            st.warning("Nenhum produto selecionado. Acesse **Gerar Cobertura** primeiro.")
            return

        # ── Apenas usuários com perfil FORNECEDOR + marca associada ──
        df_forn_db = self.db.buscar_fornecedores()
        if df_forn_db.empty:
            st.error("Nenhum usuário com perfil Fornecedor cadastrado no sistema.")
            return

        df_forn_db['fornecedor'] = df_forn_db['fornecedor'].astype(str).str.strip()
        df_forn_db['marca_aux']  = df_forn_db['marca'].astype(str).str.strip().str.upper()

        # mapa marca → fornecedores que atendem aquela marca
        mapa_marca_forn = (
            df_forn_db[df_forn_db['marca_aux'].notna() & (df_forn_db['marca_aux'] != '')]
            .groupby('marca_aux')['fornecedor']
            .apply(lambda x: sorted(set(x)))
            .to_dict()
        )
        # mapa inverso: fornecedor → suas marcas
        mapa_forn_marcas = (
            df_forn_db[df_forn_db['marca_aux'].notna() & (df_forn_db['marca_aux'] != '')]
            .groupby('fornecedor')['marca_aux']
            .apply(set)
            .to_dict()
        )

        lista_geral_fornecedores = sorted(df_forn_db['fornecedor'].unique().tolist())

        # Salva para uso no criar_pedido
        st.session_state.db_fornecedores = {
            row['fornecedor']: {'marca': row['marca']}
            for _, row in df_forn_db.iterrows()
        }

        # ── Prepara dados ─────────────────────────────────────────
        df_master = st.session_state.dados_orcamento.copy()
        df_master['marca_aux']  = df_master['marca'].astype(str).str.strip().str.upper()
        df_master['cat_filtro'] = (
            df_master['marca'].fillna("SEM MARCA").astype(str).str.upper()
            if 'marca' in df_master.columns else "SEM MARCA"
        )
        cats = sorted([str(c) for c in df_master['cat_filtro'].unique()
                       if str(c).strip() not in ["", "None", "nan"]])

        # ── Controles ─────────────────────────────────────────────
        c1, c2, c3, c4 = st.columns([1, 1.5, 1.5, 1.5], vertical_alignment="bottom")
        with c1: filtro_positivo = st.selectbox("Filtrar Marca:", ["TODOS"] + cats)
        with c2: grupos_remover  = st.multiselect("Remover Marca:", cats)
        with c3: termo_busca     = st.text_input("Pesquisar Produto:", key="search_orc")
        with c4: modo_envio      = st.selectbox(
            "Destinar Para:",
            ["Auto (por Marca)"] + lista_geral_fornecedores
        )
        
        # Verificar produtos do fornecedor selecionado
        produtos_possui = set()
        produtos_nao_possui = set()
        if modo_envio != "Auto (por Marca)":
            lista_ids = df_master['idproduto'].unique().tolist()
            produtos_possui, produtos_nao_possui = self.db.verificar_produtos_fornecedor(modo_envio, lista_ids)
            
            if produtos_nao_possui:
                st.warning(f"⚠️ {len(produtos_nao_possui)} item(ns) não disponível(is) para {modo_envio} (marca incompatível). Serão distribuídos para outros fornecedores.")

        # ── Atribui fornecedor por linha ──────────────────────────
        FORNECEDOR_PADRAO = "Natan Zimmer 99"

        def _sugerir_por_marca(marca_up):
            """Retorna fornecedores que atendem esta marca. Sem marca ou sem match → fornecedor padrão."""
            if not marca_up or marca_up in ('', 'NAN', 'NONE', 'NAO DEFINIDO'):
                return [FORNECEDOR_PADRAO]
            resultado = mapa_marca_forn.get(marca_up, [])
            return resultado if resultado else [FORNECEDOR_PADRAO]

        if modo_envio == "Auto (por Marca)":
            df_master['Fornecedor'] = df_master['marca_aux'].apply(_sugerir_por_marca)
        else:
            # Atribui fornecedor apenas para produtos que ele possui
            df_master['Fornecedor'] = df_master.apply(
                lambda row: [modo_envio] if row['idproduto'] in produtos_possui
                            else _sugerir_por_marca(row['marca_aux']),
                axis=1
            )

        st.session_state.dados_orcamento = df_master

        st.info("💡 Colunas com '✏️' são editáveis.")

        # ── Tabela ────────────────────────────────────────────────
        df_view = df_master.copy()
        
        # Filtrar apenas produtos que o fornecedor possui (se não for Auto)
        if modo_envio != "Auto (por Marca)" and produtos_possui:
            df_view = df_view[df_view['idproduto'].isin(produtos_possui)]
        
        if grupos_remover:
            df_view = df_view[~df_view['cat_filtro'].isin(grupos_remover)]
        if filtro_positivo != "TODOS":
            df_view = df_view[df_view['cat_filtro'] == filtro_positivo]
        if termo_busca:
            df_view = df_view[df_view['produto'].astype(str).str.upper().str.contains(termo_busca.upper())]
        df_view = df_view.reset_index(drop=True)

        st.data_editor(
            df_view[['idproduto', 'produto', 'marca', 'grupo', 'Fornecedor', 'Qtd Compra']],
            column_config={
                "idproduto":  st.column_config.TextColumn("Cód.",    disabled=True),
                "produto":    st.column_config.TextColumn("Produto", disabled=True, width="large"),
                "marca":      st.column_config.TextColumn("Marca",   disabled=True),
                "grupo":      st.column_config.TextColumn("Grupo",   disabled=True),
                "Fornecedor": st.column_config.MultiselectColumn(
                    "✏️ Fornecedor", options=lista_geral_fornecedores, width="large"
                ),
                "Qtd Compra": st.column_config.NumberColumn("✏️ Quantidade", min_value=0, format="%d"),
            },
            hide_index=True, use_container_width=True,
            key="editor_orcamento_grid", on_change=self.salvar_edicoes
        )
        st.divider()

        # ── Botão Registrar ───────────────────────────────────────
        col_btn, _ = st.columns([1, 3])
        with col_btn:
            if st.button("📋 REGISTRAR PEDIDO AO FORNECEDOR", type="primary", use_container_width=True):
                df_final = st.session_state.dados_orcamento.copy()
                itens = df_final[df_final['Qtd Compra'] > 0].copy()

                if itens.empty:
                    st.warning("Preencha a quantidade de pelo menos um item.")
                    return

                # Agrupa itens por fornecedor (respeitando a marca de cada um)
                cliente_nome = st.session_state.get('nome_usuario', 'Cliente')
                pedidos_por_forn: dict = {}
                for _, row in itens.iterrows():
                    forn_lista = row.get('Fornecedor', [])
                    if not isinstance(forn_lista, list) or len(forn_lista) == 0:
                        continue
                    for forn in forn_lista:
                        pedidos_por_forn.setdefault(forn, []).append(row)

                if not pedidos_por_forn:
                    st.warning("Nenhum item com fornecedor atribuído. Verifique o cadastro de marcas.")
                    return

                enviados, erros = 0, []
                # Gerar idcobertura único — combinação de timestamp + contador de sessão
                # evita colisão se dois usuários enviarem no mesmo milissegundo
                st.session_state['_idcob_counter'] = st.session_state.get('_idcob_counter', 0) + 1
                idcobertura = (int(time.time() * 1000) + st.session_state['_idcob_counter']) % MAX_INT_POSTGRES
                
                with st.spinner("Registrando pedidos..."):
                    for forn, linhas in pedidos_por_forn.items():
                        df_forn_itens = pd.DataFrame(linhas)
                        try:
                            numero = self.db.criar_pedido(cliente_nome, forn, df_forn_itens, idcobertura)
                            enviados += 1
                            logger.info(f"Pedido {numero} → {forn} ({len(linhas)} itens)")
                        except Exception as e:
                            erros.append(forn)
                            st.error(f"❌ Erro ao registrar pedido para **{forn}**: `{e}`")

                if enviados:
                    st.toast(f"✅ {enviados} pedido(s) enviado(s) com sucesso!", icon="✅")
                    st.session_state.dados_orcamento = None
                if erros:
                    st.error(f"Falha ao registrar pedido para: {', '.join(erros)}")

    def tela_pedidos_confirmados(self):
        """Tela para fornecedor visualizar pedidos confirmados pelo cliente"""
        perfil = st.session_state.get('perfil_usuario', 'FORNECEDOR')
        nome_usuario = st.session_state.get('nome_usuario', '')

        st.markdown('<h1 style="color:black; font-weight:900;">📦 Pedidos</h1>', unsafe_allow_html=True)

        st.markdown("""
            <style>
            .black-text { color: #333333 !important; font-weight: 500; }
            .pedido-num-bold { color: #0047AB !important; font-weight: 900; }
            .header-title { color: #000000 !important; font-weight: 800; font-size: 1rem; }
            .stDownloadButton button {
                background: linear-gradient(135deg, #0047AB 0%, #000000 150%) !important;
                color: white !important;
                border: none !important;
                font-weight: 700 !important;
                transition: transform 0.2s ease-in-out !important;
            }
            .stDownloadButton button:hover {
                background: linear-gradient(135deg, #0047AB 0%, #000000 150%) !important;
                color: white !important;
                transform: scale(1.02) !important;
            }
            </style>
        """, unsafe_allow_html=True)
        
        # Verificar se há pedido aberto
        pedido_aberto = st.session_state.get('pedido_confirmado_aberto')
        
        if pedido_aberto:
            # MODO DETALHE - Mostrar apenas o pedido aberto
            cache_key_confirmados = f'lista_pedidos_confirmados_{nome_usuario}_{perfil}'
            if cache_key_confirmados in st.session_state:
                df_pedidos = st.session_state[cache_key_confirmados]
            else:
                if perfil == "ADM":
                    df_pedidos = self.db.buscar_pedidos_confirmados()
                else:
                    df_pedidos = self.db.buscar_pedidos_confirmados(nome_usuario)
            
            if not df_pedidos.empty:
                row_pedido = df_pedidos[df_pedidos['id'] == pedido_aberto].iloc[0]
                num = str(row_pedido['numero_pedido'])
                fornecedor = str(row_pedido['fornecedor_nome'])
                
                st.markdown(f'<h2 style="color:black; font-weight:900;">Detalhes do Pedido #{num}</h2>', unsafe_allow_html=True)
                
                df_itens = self.db.buscar_itens_pedido(pedido_aberto)
                if not df_itens.empty:
                    st.dataframe(df_itens[['codigo_produto', 'nome_produto', 'quantidade']], use_container_width=True, hide_index=True)
                
                col_pdf, col_fechar = st.columns(2)
                with col_pdf:
                    pdf_bytes = self.gerar_pdf_pedido(fornecedor, df_itens)
                    st.download_button("📄 Baixar PDF", pdf_bytes, f"Pedido_{num}.pdf", "application/pdf", use_container_width=True)
                with col_fechar:
                    if st.button("← Voltar para Lista", type="secondary", use_container_width=True):
                        del st.session_state['pedido_confirmado_aberto']
                        # Limpar cache para recarregar
                        cache_key_confirmados = f'lista_pedidos_confirmados_{nome_usuario}_{perfil}'
                        if cache_key_confirmados in st.session_state:
                            del st.session_state[cache_key_confirmados]
                        st.rerun()
            return

        # MODO LISTA - Buscar pedidos confirmados do banco COM CACHE
        cache_key_confirmados = f'lista_pedidos_confirmados_{nome_usuario}_{perfil}'
        if cache_key_confirmados not in st.session_state:
            if perfil == "ADM":
                st.session_state[cache_key_confirmados] = self.db.buscar_pedidos_confirmados()
            else:
                st.session_state[cache_key_confirmados] = self.db.buscar_pedidos_confirmados(nome_usuario)
        df_pedidos = st.session_state[cache_key_confirmados]
        
        if df_pedidos.empty:
            st.info("Nenhum pedido confirmado disponível no momento.")
            return

        # Lista de pedidos confirmados
        with st.container(border=True):
            c_num, c_cli, c_data, c_acao = st.columns([2, 3, 2, 2])
            c_num.markdown('<div class="header-title">Pedido</div>', unsafe_allow_html=True)
            c_cli.markdown('<div class="header-title">Cliente</div>', unsafe_allow_html=True)
            c_data.markdown('<div class="header-title">Data</div>', unsafe_allow_html=True)
            c_acao.markdown('<div class="header-title">Ação</div>', unsafe_allow_html=True)
            st.markdown("<div style='margin-bottom: 10px;'></div>", unsafe_allow_html=True)

            for _, row in df_pedidos.iterrows():
                pedido_id = int(row['id'])
                num = str(row['numero_pedido'])
                cliente = str(row['cliente_nome'])
                data_str = pd.to_datetime(row['data_criacao']).strftime('%d/%m/%Y') if row['data_criacao'] else '-'
                
                # Verificar se este pedido está aberto
                pedido_aberto = st.session_state.get('pedido_confirmado_aberto')
                esta_aberto = (pedido_aberto == pedido_id)
                
                # Estilo de fundo apenas para coluna de pedido
                estilo_pedido = 'background-color: #E3F2FD; border-left: 4px solid #0047AB; padding: 8px; border-radius: 5px;' if esta_aberto else 'padding-top:8px;'
                
                col_n, col_c, col_d, col_a = st.columns([2, 3, 2, 2])
                col_n.markdown(f'<div style="{estilo_pedido}"><div class="pedido-num-bold">#{num}</div></div>', unsafe_allow_html=True)
                col_c.markdown(f'<div class="black-text" style="padding-top:8px;">{cliente}</div>', unsafe_allow_html=True)
                col_d.markdown(f'<div class="black-text" style="padding-top:8px;">{data_str}</div>', unsafe_allow_html=True)
                
                if col_a.button("Ver Detalhes", key=f"ver_conf_{pedido_id}", use_container_width=True):
                    st.session_state['pedido_confirmado_aberto'] = pedido_id
                    self._marcar_pedido_visualizado(pedido_id)
                    st.rerun()
    
    def _renderizar_detalhes_pedido_confirmado(self, pedido_aberto, nome_usuario, perfil):
        cache_key_confirmados = f'lista_pedidos_confirmados_{nome_usuario}_{perfil}'
        if cache_key_confirmados in st.session_state:
            df_pedidos = st.session_state[cache_key_confirmados]
        else:
            if perfil == "ADM":
                df_pedidos = self.db.buscar_pedidos_confirmados()
            else:
                df_pedidos = self.db.buscar_pedidos_confirmados(nome_usuario)
        
        if not df_pedidos.empty:
            row_pedido = df_pedidos[df_pedidos['id'] == pedido_aberto].iloc[0]
            num = str(row_pedido['numero_pedido'])
            fornecedor = str(row_pedido['fornecedor_nome'])
            
            st.markdown(f'<h2 style="color:black; font-weight:900;">Detalhes do Pedido #{num}</h2>', unsafe_allow_html=True)
            
            if st.button("← Voltar para Lista", type="secondary", use_container_width=False):
                del st.session_state['pedido_confirmado_aberto']
                cache_key_confirmados = f'lista_pedidos_confirmados_{nome_usuario}_{perfil}'
                if cache_key_confirmados in st.session_state:
                    del st.session_state[cache_key_confirmados]
                st.rerun()
            
            st.divider()
            
            df_itens = self.db.buscar_itens_pedido(pedido_aberto)
            if not df_itens.empty:
                st.dataframe(df_itens[['codigo_produto', 'nome_produto', 'quantidade']], use_container_width=True, hide_index=True)
            
            col_pdf, _ = st.columns([1, 3])
            with col_pdf:
                pdf_bytes = self.gerar_pdf_pedido(fornecedor, df_itens)
                st.download_button("📄 Baixar PDF", pdf_bytes, f"Pedido_{num}.pdf", "application/pdf", use_container_width=True)

    def tela_pedidos_fornecedor(self):
        perfil = st.session_state.get('perfil_usuario', 'FORNECEDOR')
        nome_usuario = st.session_state.get('nome_usuario', '')

        st.markdown('<h1 style="color:black; font-weight:900;">📋 Orçamento</h1>', unsafe_allow_html=True)
        
        if 'pedidos_com_alteracao' not in st.session_state:
            st.session_state['pedidos_com_alteracao'] = set()

        cache_key_pedidos = f'lista_pedidos_fornecedor_{nome_usuario}_{perfil}'
        if cache_key_pedidos not in st.session_state:
            if perfil == "ADM":
                st.session_state[cache_key_pedidos] = self.db.buscar_pedidos_fornecedor()
            else:
                st.session_state[cache_key_pedidos] = self.db.buscar_pedidos_fornecedor(nome_usuario)
        df_pedidos = st.session_state[cache_key_pedidos]

        if df_pedidos.empty:
            st.info("Nenhum pedido disponível no momento.")
            return

        st.markdown("""
            <style>
            .black-text { color: #333333 !important; font-weight: 500; }
            .pedido-num-bold { color: #0047AB !important; font-weight: 900; }
            .header-title { color: #000000 !important; font-weight: 800; font-size: 1rem; }
            .stDownloadButton button {
                background: linear-gradient(135deg, #0047AB 0%, #000000 150%) !important;
                color: white !important;
                border: none !important;
                font-weight: 700 !important;
                transition: transform 0.2s ease-in-out !important;
            }
            .stDownloadButton button:hover {
                background: linear-gradient(135deg, #0047AB 0%, #000000 150%) !important;
                color: white !important;
                transform: scale(1.02) !important;
            }
            </style>
        """, unsafe_allow_html=True)
        
        pedido_aberto = st.session_state.get('pedido_aberto')
        
        if pedido_aberto:
            self._renderizar_detalhes_pedido_fornecedor(pedido_aberto, df_pedidos, nome_usuario, perfil)
            return
        
        st.info("INFO: 🟢 **Verde:** Orçamento enviado | 🟡 **Amarelo:** Pedido recente (até 2 dias) | 🟠 **Laranja:** Atenção necessária (3-6 dias) | 🔴 **Vermelho:** Urgente (7+ dias)")

        # 3. CABEÇALHO DA LISTA COM BORDA
        with st.container(border=True):
            c_num, c_cli, c_status, c_data, c_acao = st.columns([1.5, 2.5, 1.5, 1.8, 1.5])
            c_num.markdown('<div class="header-title">Pedido</div>', unsafe_allow_html=True)
            c_cli.markdown('<div class="header-title">Cliente</div>', unsafe_allow_html=True)
            c_status.markdown('<div class="header-title" style="text-align: center;">Status</div>', unsafe_allow_html=True)
            c_data.markdown('<div class="header-title">Data</div>', unsafe_allow_html=True)
            c_acao.markdown('<div class="header-title">Ação</div>', unsafe_allow_html=True)
            st.markdown("<div style='margin-bottom: 10px;'></div>", unsafe_allow_html=True)

            # 4. LISTAGEM DE PEDIDOS (Com borda ao redor de cada um)
            # FIX N+1: pre-carregar status visualizado e urgente em batch antes do loop
            _todos_ids = df_pedidos["id"].astype(int).tolist()
            _ids_visualizados_batch = set()
            _ids_urgentes_batch = set()
            try:
                with self.db.get_connection() as _conn:
                    _cur = _conn.cursor()
                    _cur.execute("SELECT pedido_id FROM pedidos_visualizados WHERE LOWER(usuario) = LOWER(%s) AND pedido_id = ANY(%s)", (nome_usuario, [str(i) for i in _todos_ids]))
                    _ids_visualizados_batch = {r[0] for r in _cur.fetchall()}
                    _cur.execute("SELECT DISTINCT pv.id FROM pedidos_vendas pv JOIN notificacoes_fornecedor nf ON pv.numero_pedido = nf.numero_pedido WHERE pv.id = ANY(%s) AND LOWER(nf.fornecedor) = LOWER(%s)", (_todos_ids, nome_usuario))
                    _ids_urgentes_batch = {r[0] for r in _cur.fetchall()}
            except Exception as e:
                logger.error(f"Erro ao pre-carregar status de pedidos: {e}")
            for _, row in df_pedidos.iterrows():
                pedido_id  = int(row['id'])
                num        = str(row['numero_pedido'])
                cliente    = str(row['cliente_nome'])
                status     = str(row['status'])
                data_criacao = pd.to_datetime(row['data_criacao'])
                # FIX: normalizar timezone
                if data_criacao.tzinfo is not None:
                    data_criacao = data_criacao.tz_localize(None)
                data_str   = data_criacao.strftime('%d/%m/%Y')
                
                # Calcular dias desde criação
                dias_desde_criacao = (datetime.datetime.now() - data_criacao).days
                
                # Badge de status com cores dinâmicas para Pendente
                if status == "Enviado":
                    badge_bg, badge_txt = "#D4EDDA", "#155724"  # Verde
                else:  # Pendente
                    if dias_desde_criacao >= 7:
                        badge_bg, badge_txt = "#FFCCCC", "#8B0000"  # Vermelho
                    elif dias_desde_criacao >= 3:
                        badge_bg, badge_txt = "#FFE5CC", "#CC5500"  # Laranja
                    else:
                        badge_bg, badge_txt = "#FFF3CD", "#856404"  # Amarelo

                # Verificar se este pedido está aberto
                pedido_aberto = st.session_state.get('pedido_aberto')
                esta_aberto = (pedido_aberto == pedido_id)
                
                # FIX N+1: usar conjuntos pre-carregados em vez de query por pedido
                eh_novo    = str(pedido_id) not in _ids_visualizados_batch
                eh_urgente = pedido_id in _ids_urgentes_batch
                
                # Estilo de fundo apenas para coluna de pedido
                estilo_pedido = 'background-color: #E3F2FD; border-left: 4px solid #0047AB; padding: 8px; border-radius: 5px;' if esta_aberto else 'padding-top:8px;'

                col_n, col_c, col_s, col_d, col_a = st.columns([1.5, 2.5, 1.5, 1.8, 1.5])
                
                # Adicionar badge URGENTE ou NOVO na lista
                if eh_urgente:
                    badge_lista = ' <span style="background:#DC3545;color:#FFFFFF;padding:2px 8px;border-radius:8px;font-size:0.7rem;font-weight:bold;margin-left:6px;">URGENTE</span>'
                elif eh_novo:
                    badge_lista = ' <span style="background:#FFC107;color:#000000;padding:2px 8px;border-radius:8px;font-size:0.7rem;font-weight:bold;margin-left:6px;">NOVO</span>'
                else:
                    badge_lista = ''
                col_n.markdown(f'<div style="{estilo_pedido}"><div class="pedido-num-bold">#{num}{badge_lista}</div></div>', unsafe_allow_html=True)
                col_c.markdown(f'<div class="black-text" style="padding-top:8px;">{cliente}</div>', unsafe_allow_html=True)
                col_s.markdown(f'''
                    <div style="background:{badge_bg}; color:{badge_txt}; padding:4px 8px; 
                    border-radius:12px; font-size:0.8rem; font-weight:bold; text-align:center; margin-top:8px;">
                        {status}
                    </div>
                ''', unsafe_allow_html=True)
                col_d.markdown(f'<div class="black-text" style="padding-top:8px;">{data_str}</div>', unsafe_allow_html=True)
                
                texto_botao = "✖ Fechar" if esta_aberto else ("Visualizar" if status == "Enviado" else "Ver / Responder")
                
                if col_a.button(texto_botao, key=f"ver_{pedido_id}", use_container_width=True):
                    if pedido_aberto == pedido_id:
                        del st.session_state['pedido_aberto']
                        if 'pedido_num' in st.session_state:
                            del st.session_state['pedido_num']
                    else:
                        st.session_state['pedido_aberto'] = pedido_id
                        st.session_state['pedido_num']    = num
                        self._marcar_pedido_visualizado(pedido_id)
                        # Remover notificação de cobrança da sidebar (mas manter flag de urgente)
                        try:
                            with self.db.get_connection() as conn:
                                cur = conn.cursor()
                                cur.execute("""
                                    UPDATE notificacoes_fornecedor 
                                    SET lida = TRUE 
                                    WHERE numero_pedido = %s AND LOWER(fornecedor) = LOWER(%s)
                                """, (num, nome_usuario))
                                conn.commit()
                        except Exception as e:
                            logger.error(f"Erro ao marcar notificação como lida: {e}")
                    st.rerun()
    
    def _renderizar_detalhes_pedido_fornecedor(self, pedido_id, df_pedidos, nome_usuario, perfil):
        if 'pedido_aberto' in st.session_state:
            num = st.session_state.get('pedido_num', pedido_id)
            
            # Buscar status do pedido
            row_pedido = df_pedidos[df_pedidos['id'] == pedido_id]
            status_pedido = row_pedido['status'].values[0] if not row_pedido.empty else "Pendente"
            
            if st.button("← Voltar para Lista", type="secondary", use_container_width=False):
                del st.session_state['pedido_aberto']
                if 'pedido_num' in st.session_state:
                    del st.session_state['pedido_num']
                cache_key_pedidos = f'lista_pedidos_fornecedor_{nome_usuario}_{perfil}'
                if cache_key_pedidos in st.session_state:
                    del st.session_state[cache_key_pedidos]
                st.rerun()
            
            st.divider()
            # Verificar se é urgente e exibir badge
            eh_urgente = self._pedido_eh_urgente(pedido_id, nome_usuario)
            eh_novo = self._pedido_eh_novo(pedido_id)
            
            if eh_urgente:
                badge = ' <span style="background:#DC3545;color:#FFFFFF;padding:4px 12px;border-radius:12px;font-size:0.8rem;font-weight:bold;margin-left:10px;">URGENTE</span>'
            elif eh_novo:
                badge = ' <span style="background:#FFC107;color:#000000;padding:4px 12px;border-radius:12px;font-size:0.8rem;font-weight:bold;margin-left:10px;">NOVO</span>'
            else:
                badge = ''
            
            st.markdown(f'<h2 style="color:black; font-weight:900;">Detalhes do Pedido #{num}{badge}</h2>', unsafe_allow_html=True)
            
            # Verificar se este pedido teve alteração de quantidade
            if pedido_id in st.session_state.get('pedidos_com_alteracao', set()):
                st.warning("⚠️ ATENÇÃO: Houve alteração na quantidade de alguns itens deste pedido.")

            df_itens = self.db.buscar_itens_pedido(pedido_id)
            if df_itens.empty:
                st.warning("Nenhum item encontrado para este pedido.")
            else:
                # MODO VISUALIZAÇÃO (Status Enviado)
                if status_pedido == "Enviado":
                    st.info("👁️ Orçamento já enviado. Visualização somente leitura.")
                    
                    # Buscar observação
                    try:
                        with self.db.get_connection() as conn:
                            cur = conn.cursor()
                            cur.execute("""
                                SELECT observacao FROM pedidos_vendas WHERE id = %s
                            """, (pedido_id,))
                            result = cur.fetchone()
                            observacao_salva = result[0] if result and result[0] else ""
                    except Exception as e:
                        logger.error(f"Erro ao buscar observação: {e}")
                        observacao_salva = ""
                    
                    # Exibir tabela somente leitura
                    st.dataframe(
                        df_itens[['codigo_produto', 'nome_produto', 'quantidade', 'valor_unitario', 'impostos', 'frete', 'prazo_entrega']],
                        column_config={
                            "codigo_produto": "Código",
                            "nome_produto": "Produto",
                            "quantidade": "Qtd",
                            "valor_unitario": st.column_config.NumberColumn("Vl. Unit.", format="R$ %.2f"),
                            "impostos": st.column_config.NumberColumn("Impostos", format="R$ %.2f"),
                            "frete": "Frete",
                            "prazo_entrega": "Prazo"
                        },
                        use_container_width=True,
                        hide_index=True
                    )
                    
                    # Cálculo de Total
                    df_itens['_total'] = (df_itens['quantidade'] * df_itens['valor_unitario'].fillna(0)) + df_itens['impostos'].fillna(0)
                    total_geral = df_itens['_total'].sum()
                    st.markdown(f'<div style="text-align:right; font-size:1.2rem; font-weight:900; color:#0047AB;">Total: R$ {total_geral:,.2f}</div>', unsafe_allow_html=True)
                    st.write("")
                    
                    # Exibir observação (somente leitura)
                    if observacao_salva:
                        st.info(f"📝 **Observação:** {observacao_salva}")
                    
                    # Botões: PDF e Fechar
                    col_pdf, col_fechar = st.columns(2)
                    with col_pdf:
                        forn_nome = row_pedido['fornecedor_nome'].values[0] if not row_pedido.empty else "Fornecedor"
                        pdf_bytes = self.gerar_pdf_pedido(forn_nome, df_itens)
                        st.download_button("📄 Baixar PDF", pdf_bytes, f"Pedido_{num}.pdf", "application/pdf", use_container_width=True)
                    with col_fechar:
                        if st.button("✖ Fechar", type="secondary", use_container_width=True, key=f"close_view_{pedido_id}"):
                            del st.session_state['pedido_aberto']
                            cache_key_pedidos = f'lista_pedidos_fornecedor_{nome_usuario}_{perfil}'
                            if cache_key_pedidos in st.session_state:
                                del st.session_state[cache_key_pedidos]
                            st.rerun()
                
                # MODO EDIÇÃO (Status Pendente)
                else:
                    st.info("✏️ Preencha os campos abaixo e confirme a resposta.")
                    
                    # Campos de preenchimento rápido e pesquisa
                    col1, col2, col3, col4 = st.columns([2, 1.5, 1.5, 1.5])
                    with col1:
                        pesquisa_produto = st.text_input("Pesquisar produto", key=f"pesquisa_prod_{pedido_id}")
                    with col2:
                        valor_global = st.number_input("Preencher Vl. Unit.", min_value=0.0, step=0.01, key=f"valor_global_{pedido_id}")
                    with col3:
                        imposto_global = st.number_input("Preencher Impostos", min_value=0.0, step=0.01, key=f"imposto_global_{pedido_id}")
                    with col4:
                        frete_global = st.selectbox("Preencher Frete", ["", "CIF", "FOB"], key=f"frete_global_{pedido_id}")
                    
                    col_data, _ = st.columns([2, 8])
                    with col_data:
                        data_global = st.date_input("Preencher todas as datas", key=f"data_global_{pedido_id}", value=None)
                
                    
                    # Filtrar itens se houver pesquisa
                    df_itens_filtrado = df_itens.copy()
                    if pesquisa_produto:
                        df_itens_filtrado = df_itens[
                            df_itens['codigo_produto'].astype(str).str.contains(pesquisa_produto, case=False, na=False) |
                            df_itens['nome_produto'].astype(str).str.contains(pesquisa_produto, case=False, na=False)
                        ]
                    
                    if df_itens_filtrado.empty:
                        st.warning("Nenhum produto encontrado com os termos de pesquisa.")
                        df_itens_filtrado = df_itens  # Mostra todos se não encontrar
                    
                    # Converter prazo_entrega para datetime para usar DateColumn
                    df_itens_filtrado['prazo_entrega'] = pd.to_datetime(df_itens_filtrado['prazo_entrega'], errors='coerce')
                    
                    # Preencher todas as datas se data_global foi informada
                    if data_global:
                        df_itens_filtrado['prazo_entrega'] = pd.to_datetime(data_global)
                    
                    # Preencher valores globais se informados
                    if valor_global > 0:
                        df_itens_filtrado['valor_unitario'] = valor_global
                    if imposto_global > 0:
                        df_itens_filtrado['impostos'] = imposto_global
                    if frete_global:
                        df_itens_filtrado['frete'] = frete_global

                    df_edit = st.data_editor(
                        df_itens_filtrado,
                        column_config={
                            "id": None,
                            "codigo_produto": st.column_config.TextColumn("Codigo", disabled=True),
                            "nome_produto":   st.column_config.TextColumn("Produto", disabled=True, width="large"),
                            "quantidade":     st.column_config.NumberColumn("Qtd",  disabled=True),
                            "valor_unitario": st.column_config.NumberColumn("✏️ Vl. Unit.", format="R$ %.2f", min_value=0.0),
                            "impostos":       st.column_config.NumberColumn("✏️ Impostos", format="R$ %.2f", min_value=0.0),
                            "frete":          st.column_config.SelectboxColumn("✏️ Frete", options=["CIF", "FOB"]),
                            "prazo_entrega":  st.column_config.DateColumn("✏️ Prazo", format="DD/MM/YYYY"),
                        },
                        hide_index=True, use_container_width=True, key=f"editor_{pedido_id}"
                    )

                    # Cálculo de Total (frete agora é texto, não soma)
                    df_edit['_total'] = (df_edit['quantidade'] * df_edit['valor_unitario'].fillna(0)) + \
                                         df_edit['impostos'].fillna(0)
                    total_geral = df_edit['_total'].sum()

                    st.markdown(f'<div style="text-align:right; font-size:1.2rem; font-weight:900; color:#0047AB;">Total: R$ {total_geral:,.2f}</div>', unsafe_allow_html=True)
                    st.write("")
                    
                    # Campo de observação
                    observacao = st.text_area(
                        "Observação (opcional)",
                        key=f"obs_{pedido_id}",
                        placeholder="Digite aqui observações sobre o orçamento (prazos, condições, etc.)",
                        height=100
                    )
                    st.write("")

                    col_pdf, col_confirm, col_fechar = st.columns([1, 1, 1])

                    with col_pdf:
                        # Lógica do PDF
                        row_pedido = df_pedidos[df_pedidos['id'] == pedido_id]
                        forn_nome = row_pedido['fornecedor_nome'].values[0] if not row_pedido.empty else "Fornecedor"
                        pdf_bytes = self.gerar_pdf_pedido(forn_nome, df_edit)
                        st.download_button("📄 Baixar PDF", pdf_bytes, f"Pedido_{num}.pdf", "application/pdf", use_container_width=True)

                    with col_confirm:
                        if st.button("✅ Confirmar Resposta", type="primary", use_container_width=True, key=f"confirm_{pedido_id}"):
                            st.session_state[f'confirmar_resposta_{pedido_id}'] = True
                            st.rerun()
                    
                    # Pop-up de confirmação
                    if st.session_state.get(f'confirmar_resposta_{pedido_id}', False):
                        @st.dialog("Confirmar Envio de Orçamento")
                        def confirmar_resposta():
                            st.warning(f"📦 Deseja confirmar o envio do orçamento do pedido **#{num}**?")
                            st.info(f"💰 Total: **R$ {total_geral:,.2f}**")
                            if observacao:
                                st.info(f"📝 Observação: {observacao}")
                            col1, col2 = st.columns(2)
                            with col1:
                                if st.button("✅ Confirmar", type="primary", use_container_width=True):
                                    if self.db.salvar_resposta_pedido(pedido_id, df_edit, observacao):
                                        del st.session_state[f'confirmar_resposta_{pedido_id}']
                                        st.toast(f"✅ Orçamento do pedido #{num} enviado!", icon="✅")
                                        del st.session_state['pedido_aberto']
                                        # FIX: invalidar cache para refletir status Enviado na lista
                                        cache_key = f'lista_pedidos_fornecedor_{nome_usuario}_{perfil}'
                                        if cache_key in st.session_state:
                                            del st.session_state[cache_key]
                                        st.rerun()
                            with col2:
                                if st.button("❌ Cancelar", type="secondary", use_container_width=True):
                                    del st.session_state[f'confirmar_resposta_{pedido_id}']
                                    st.rerun()
                        confirmar_resposta()

                    with col_fechar:
                        if st.button("✖ Fechar", type="secondary", use_container_width=True, key=f"close_{pedido_id}"):
                            del st.session_state['pedido_aberto']
                            # Limpar cache para recarregar lista
                            cache_key_pedidos = f'lista_pedidos_fornecedor_{nome_usuario}_{perfil}'
                            if cache_key_pedidos in st.session_state:
                                del st.session_state[cache_key_pedidos]
                            st.rerun()

    def _renderizar_filtros_cobertura(self):
        """Renderiza filtros em cascata da tela de cobertura"""
        c1, c2, c3 = st.columns(3, vertical_alignment="bottom") 
        c4, c5, c6 = st.columns(3, vertical_alignment="bottom")

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
        
        return v_filial, v_marca, v_grupo, v_subgrupo, v_subgrupo1, v_produto
    
    def _renderizar_parametros_analise(self, modo, c_p1, c_p2, c_btn):
        """Renderiza parâmetros de análise (dias alvo, corte, etc)"""
        incluir_sem_venda = False
        
        if modo == "Sugestão de Compra":
            with c_p1: dias_alvo = st.number_input("Cobertura Alvo (Dias)", min_value=1, value=30)
            with c_p2: dias_corte = st.number_input("Estoque Mínimo (Dias)", min_value=0, value=10)
            modo_db = 'COMPRA'
        else:
            dias_alvo = 30
            with c_p1: dias_corte = st.number_input("Estoque Máximo (Dias)", min_value=0, value=45)
            with c_p2: incluir_sem_venda = st.checkbox("Incluir não vendidos?", key="chk_venda_v6", value=False)
            modo_db = 'SOBRA'
        
        return dias_alvo, dias_corte, modo_db, incluir_sem_venda
    
    def _processar_envio_pedidos(self, df_envio, modo_envio, fornecedores_destino, lista_fornecedores, df_forn_db):
        """Processa envio de pedidos para fornecedores"""
        cliente_nome = st.session_state.get('nome_usuario', 'Cliente')
        enviados = 0
        erros = []
        st.session_state['_idcob_counter'] = st.session_state.get('_idcob_counter', 0) + 1
        idcobertura = (int(time.time() * 1000) + st.session_state['_idcob_counter']) % MAX_INT_POSTGRES
        
        with st.spinner("Enviando pedidos..."):
            if modo_envio == "Todos os Fornecedores":
                for forn in fornecedores_destino:
                    try:
                        df_forn = df_envio.copy()
                        marcas_forn = df_forn_db[df_forn_db['fornecedor'] == forn]['marca'].str.upper().tolist()
                        if marcas_forn and 'marca' in df_forn.columns:
                            df_forn = df_forn[df_forn['marca'].str.upper().isin(marcas_forn)]
                        
                        if not df_forn.empty:
                            df_forn['Qtd Compra'] = df_forn['reposicao']
                            numero = self.db.criar_pedido(cliente_nome, forn, df_forn, idcobertura)
                            enviados += 1
                            logger.info(f"Pedido {numero} enviado para {forn} com {len(df_forn)} itens")
                    except Exception as e:
                        erros.append(forn)
                        logger.error(f"Erro ao enviar para {forn}: {e}")
            else:
                for forn in fornecedores_destino:
                    try:
                        if modo_envio == "Pré Definido":
                            df_forn = df_envio[df_envio['fornecedor'] == forn].copy()
                        elif modo_envio == "Fornecedor Único":
                            df_forn = df_envio.copy()
                        else:
                            df_forn = df_envio.copy()
                            marcas_forn = df_forn_db[df_forn_db['fornecedor'] == forn]['marca'].str.upper().tolist()
                            if marcas_forn and 'marca' in df_forn.columns:
                                df_forn = df_forn[df_forn['marca'].str.upper().isin(marcas_forn)]
                        
                        if not df_forn.empty:
                            df_forn['Qtd Compra'] = df_forn['reposicao']
                            numero = self.db.criar_pedido(cliente_nome, forn, df_forn, idcobertura)
                            enviados += 1
                            logger.info(f"Pedido {numero} enviado para {forn} com {len(df_forn)} itens")
                    except Exception as e:
                        erros.append(forn)
                        logger.error(f"Erro ao enviar para {forn}: {e}")
        
        return enviados, erros
    
    def tela_cobertura(self):
        st.markdown("""
            <style>
                [data-baseweb="select"] * { color: #000000 !important; -webkit-text-fill-color: #000000 !important; }
                .align-btn { margin-top: 28px; }
                [data-testid="stMarkdownContainer"] h3 { color: #000000 !important; }
                [data-testid="stWidgetLabel"] { color: #000000 !important; }
                /* Exceção para multiselect de fornecedores */
                div[data-testid="stMultiSelect"] [data-baseweb="select"] {
                    background-color: #F8F9FA !important;
                }
                /* Botão de filtro sem fornecedor */
                section[data-testid="stMain"] div.stButton > button[kind="secondary"] {
                    transition: transform 0.2s ease-in-out, background-color 0.2s ease-in-out !important;
                }
                section[data-testid="stMain"] div.stButton > button[kind="secondary"]:hover {
                    background-color: #DCDFE4 !important;
                    color: #0047AB !important;
                    border-color: #0047AB !important;
                    transform: scale(1.05) !important;
                }
            </style>
        """, unsafe_allow_html=True)

        with st.container():
            # --- FILTROS SUPERIORES EM CASCATA ---
            v_filial, v_marca, v_grupo, v_subgrupo, v_subgrupo1, v_produto = self._renderizar_filtros_cobertura()
            
            st.write("") 

            # --- PARÂMETROS DE ANÁLISE ---
            c_tipo, c_p1, c_p2, c_btn = st.columns([1.5, 1, 1, 1.5], vertical_alignment="bottom")
            with c_tipo:
                modo = st.selectbox("Tipo de Análise", ["Sugestão de Compra", "Análise de Sobra"])
            
            dias_alvo, dias_corte, modo_db, incluir_sem_venda = self._renderizar_parametros_analise(modo, c_p1, c_p2, c_btn)

            # --- BOTÃO GERAR ANÁLISE ---
            # Detectar mudanças nos filtros
            filtros_atuais = (v_filial, v_marca, v_grupo, v_subgrupo, v_subgrupo1, v_produto, dias_alvo, dias_corte, modo_db, incluir_sem_venda)
            filtros_anteriores = st.session_state.get('filtros_anteriores', None)
            
            gerar_analise = False
            with c_btn:
                if st.button("GERAR ANÁLISE", type="primary", use_container_width=True):
                    gerar_analise = True
            
            # FIX: removida atualização automática por mudança de filtro — causava loop de reruns
            # e sobrecarga desnecessária no banco a cada interação com filtros.
            # O usuário deve clicar explicitamente em "GERAR ANÁLISE".
            
            if gerar_analise:
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
                    st.session_state.filtros_anteriores = filtros_atuais
                    if len(df) == 2000:
                        st.warning("⚠️ O resultado foi limitado a 2.000 itens. Aplique filtros mais específicos para ver todos os dados.")
                    st.rerun()

            # --- PROCESSAMENTO DOS DADOS PARA EXIBIÇÃO ---
            df_cache = st.session_state.get('df_analise_cache', pd.DataFrame())
            modo_atual = st.session_state.get('modo_analise_atual', 'COMPRA')

            if not df_cache.empty:
                st.divider()
                df_processado = df_cache.copy()

                # Renomear colunas ANTES da ordenação
                df_processado = df_processado.rename(columns={
                    'vol_estoque': 'estoque',
                    'venda_periodo': f'venda({dias_alvo}D)'
                })

                # 1. ORDENAÇÃO
                if modo_atual == 'SOBRA':
                    df_processado = df_processado.sort_values(by=['dias_estoque', 'estoque'], ascending=[False, False])
                else:
                    df_processado = df_processado.sort_values(by=['dias_estoque', 'reposicao'], ascending=[True, False])

                df_processado = df_processado.drop_duplicates(subset=['idproduto'], keep='first')

                # 2. DEFINIÇÃO DE COLUNAS
                lista_base = ['filial', 'idproduto', 'produto', 'marca', 'estoque', f'venda({dias_alvo}D)', 'dias_estoque', 'reposicao']
                ordem_visual = [c for c in lista_base if c != 'reposicao'] if modo_atual == 'SOBRA' else lista_base

                colunas_finais = [c for c in ordem_visual if c in df_processado.columns]
                outras_colunas = [c for c in df_processado.columns if c not in colunas_finais]
                df_final = df_processado[colunas_finais + outras_colunas]

                # --- RODAPÉ (AÇÕES) ---
                if modo_atual == 'SOBRA':
                    # Adicionar coluna de seleção para filtrar itens
                    if 'incluir' not in df_final.columns:
                        df_final.insert(0, 'incluir', True)
                    
                    # Aplicar itens removidos
                    itens_removidos = st.session_state.get('itens_removidos', [])
                    if itens_removidos:
                        df_final = df_final[~df_final['idproduto'].isin(itens_removidos)]
                    
                    # Botão de Rollback
                    if itens_removidos:
                        col_rollback, _ = st.columns([1, 3])
                        with col_rollback:
                            if st.button("↩️ Desfazer Última Remoção"):
                                itens_removidos.pop()
                                st.session_state.itens_removidos = itens_removidos
                                st.rerun()
                    
                    # Tabela editável
                    df_editado_sobra = st.data_editor(
                        df_final,
                        column_config={
                            "incluir": st.column_config.CheckboxColumn("Remover", help="Desmarque para remover", default=True),
                            "filial": st.column_config.TextColumn("Filial", disabled=True),
                            "idproduto": st.column_config.TextColumn("Código", disabled=True),
                            "produto": st.column_config.TextColumn("Produto", disabled=True, width="medium"),
                            "marca": st.column_config.TextColumn("Marca", disabled=True),
                            "estoque": st.column_config.NumberColumn("Estoque", disabled=True, format="%.0f und"),
                            f"venda({dias_alvo}D)": st.column_config.NumberColumn(f"Venda({dias_alvo}D)", disabled=True, format="%.0f und"),
                            "dias_estoque": st.column_config.NumberColumn("Dias Estoque", disabled=True, format="%.1f dias"),
                            "grupo": None, "subgrupo": None, "subgrupo1": None
                        },
                        use_container_width=True,
                        hide_index=True,
                        height=500,
                        key="editor_sobra"
                    )
                    
                    # Processar itens desmarcados
                    itens_desmarcados = df_editado_sobra[df_editado_sobra['incluir'] == False]['idproduto'].tolist()
                    if itens_desmarcados:
                        for item in itens_desmarcados:
                            if item not in itens_removidos:
                                itens_removidos.append(item)
                        st.session_state.itens_removidos = itens_removidos
                        st.rerun()
                    
                    col_espaco, col_acao = st.columns([3, 2])
                    with col_acao:
                        st.markdown("""
                            <style>
                            .stDownloadButton button {
                                background-color: #0047AB !important;
                                color: white !important;
                                border: none !important;
                                transition: transform 0.2s ease-in-out !important;
                            }
                            .stDownloadButton button:hover {
                                transform: scale(1.02) !important;
                            }
                            </style>
                        """, unsafe_allow_html=True)
                        df_pdf = df_editado_sobra[df_editado_sobra['incluir'] == True].copy()
                        pdf_bytes = self.gerar_pdf_sobra(df_pdf, dias_corte, dias_alvo)
                        st.download_button(
                            "📄 Baixar PDF",
                            pdf_bytes,
                            f"Sobras_{pd.Timestamp.now().strftime('%d%m')}.pdf",
                            "application/pdf",
                            type="primary",
                            use_container_width=True
                        )
                else:
                    # FILTRO DE FORNECEDORES
                    st.markdown('<h3 style="color:#000000;">Enviar para Fornecedores</h3>', unsafe_allow_html=True)
                                    
                    df_forn_db = self.db.buscar_fornecedores()
                    lista_fornecedores = sorted(df_forn_db['fornecedor'].unique().tolist()) if not df_forn_db.empty else []
                    
                    col_modo, col_selecao = st.columns([1, 2], vertical_alignment="bottom")
                    with col_modo:
                        modo_envio = st.selectbox("Modo de Envio:", ["Pré Definido", "Todos os Fornecedores", "Fornecedores Específicos", "Fornecedor Único"], key="modo_envio_cobertura")
                    
                    fornecedores_selecionados = []
                    fornecedor_unico = None
                    if modo_envio == "Fornecedores Específicos":
                        with col_selecao:
                            st.markdown("""
                                <style>
                                    div[data-testid="stMultiSelect"] [data-baseweb="select"] > div {
                                        background-color: #F8F9FA !important;
                                    }
                                </style>
                            """, unsafe_allow_html=True)
                            fornecedores_selecionados = st.multiselect("Selecione os Fornecedores:", lista_fornecedores, key="fornecedores_especificos")
                    elif modo_envio == "Fornecedor Único":
                        with col_selecao:
                            fornecedor_unico = st.selectbox("Selecione o Fornecedor:", lista_fornecedores, key="fornecedor_unico")
                    
                    # Mensagens de notificação sempre abaixo dos filtros
                    if modo_envio == "Fornecedores Específicos" and fornecedores_selecionados:
                        st.info("📌 Itens de marcas não vendidas pelos fornecedores selecionados permanecerão com o fornecedor original.")
                    elif modo_envio == "Fornecedor Único" and fornecedor_unico:
                        st.info(f"📌 Serão enviados somente itens vendidos por {fornecedor_unico}.")
                    elif modo_envio == "Todos os Fornecedores":
                        st.info("📌 Modo 'Todos': Cada item será enviado para TODOS os fornecedores que vendem sua marca.")
                    
                    # Adicionar coluna de seleção (todos marcados por padrão)
                    if 'incluir' not in df_final.columns:
                        df_final.insert(0, 'incluir', True)
                    
                    # Preencher fornecedor baseado no modo de envio
                    if 'fornecedor' in df_final.columns:
                        if modo_envio == "Todos os Fornecedores":
                            # Mostra "Todos" na coluna
                            df_final['fornecedor'] = "Todos"
                        elif modo_envio == "Fornecedores Específicos" and fornecedores_selecionados:
                            # Para cada item, verifica TODOS os fornecedores selecionados que vendem a marca
                            indices_manter = []
                            for idx, row in df_final.iterrows():
                                marca_item = str(row.get('marca', '')).upper()
                                fornecedores_que_vendem = []
                                
                                # Verifica quais fornecedores selecionados vendem esta marca
                                for forn_sel in fornecedores_selecionados:
                                    marcas_forn = df_forn_db[df_forn_db['fornecedor'].str.strip() == forn_sel.strip()]['marca'].str.upper().tolist()
                                    if marca_item in marcas_forn:
                                        fornecedores_que_vendem.append(forn_sel)
                                
                                # Se pelo menos um fornecedor vende, adiciona o item
                                if fornecedores_que_vendem:
                                    df_final.at[idx, 'fornecedor'] = ', '.join(sorted(fornecedores_que_vendem))
                                    indices_manter.append(idx)
                            
                            # Remove itens que nenhum fornecedor selecionado vende
                            df_final = df_final.loc[indices_manter]
                        elif modo_envio == "Fornecedor Único" and fornecedor_unico:
                            # Buscar marcas do fornecedor único
                            marcas_forn_unico = df_forn_db[df_forn_db['fornecedor'] == fornecedor_unico]['marca'].str.upper().tolist()
                            
                            # Filtrar apenas itens que o fornecedor vende
                            indices_manter = []
                            for idx, row in df_final.iterrows():
                                marca_item = str(row.get('marca', '')).upper()
                                if marca_item in marcas_forn_unico:
                                    df_final.at[idx, 'fornecedor'] = fornecedor_unico
                                    indices_manter.append(idx)
                            
                            # Remove itens que o fornecedor não vende
                            df_final = df_final.loc[indices_manter]
                        else:
                            # Pré Definido: mantém o fornecedor original
                            pass
                    
                    # Verificar itens sem fornecedor ANTES de aplicar remoções
                    if modo_envio in ["Pré Definido", "Fornecedores Específicos", "Fornecedor Único"]:
                        tem_sem_fornecedor = df_final['fornecedor'].isna().any() or (df_final['fornecedor'] == '').any()
                        if tem_sem_fornecedor:
                            qtd_sem_fornecedor = df_final[df_final['fornecedor'].isna() | (df_final['fornecedor'] == '')].shape[0]
                            st.warning(f"⚠️ {qtd_sem_fornecedor} item(ns) sem fornecedor definido. Atribua um fornecedor antes de enviar.")
                            
                            # Checkbox para filtrar
                            filtrar = st.checkbox("Mostrar apenas itens sem fornecedor", key="chk_filtrar_sem_forn")
                            st.session_state.filtrar_sem_fornecedor = filtrar
                        else:
                            # Limpar filtro se não há mais itens sem fornecedor
                            st.session_state.filtrar_sem_fornecedor = False
                    else:
                        # Limpar filtro em outros modos
                        st.session_state.filtrar_sem_fornecedor = False
                    
                    # Aplicar filtro de itens sem fornecedor se ativado
                    if st.session_state.get('filtrar_sem_fornecedor', False):
                        df_final = df_final[df_final['fornecedor'].isna() | (df_final['fornecedor'] == '')]
                    
                    # Aplicar itens removidos após filtros
                    itens_removidos = st.session_state.get('itens_removidos', [])
                    if itens_removidos:
                        df_final = df_final[~df_final['idproduto'].isin(itens_removidos)]
                    
                    # Botão de Rollback (só aparece se houver itens removidos)
                    if itens_removidos:
                        col_rollback, _ = st.columns([1, 3])
                        with col_rollback:
                            if st.button("↩️ Desfazer Última Remoção"):
                                itens_removidos.pop()
                                st.session_state.itens_removidos = itens_removidos
                                st.rerun()
                    
                    # Tabela editável com cores
                    styled_df = df_final.style
                    if 'dias_estoque' in df_final.columns:
                        styled_df = styled_df.background_gradient(cmap='Reds_r', subset=['dias_estoque'], vmin=0, vmax=30)
                    if 'reposicao' in df_final.columns:
                        styled_df = styled_df.background_gradient(cmap='Blues', subset=['reposicao'], vmin=0, vmax=100)
                    
                    # Configurar coluna fornecedor baseado no modo
                    if modo_envio == "Todos os Fornecedores":
                        config_fornecedor = st.column_config.TextColumn("Fornecedor", disabled=True)
                    elif modo_envio == "Fornecedores Específicos":
                        config_fornecedor = st.column_config.TextColumn("Fornecedor(es)", disabled=True, help="Itens serão enviados para todos os fornecedores listados")
                    else:
                        config_fornecedor = st.column_config.SelectboxColumn("✏️ Fornecedor", options=lista_fornecedores, required=True)
                    
                    df_editado = st.data_editor(
                        styled_df,
                        column_config={
                            "incluir": st.column_config.CheckboxColumn("Remover", help="Desmarque para remover", default=True),
                            "filial": st.column_config.TextColumn("Filial", disabled=True),
                            "idproduto": st.column_config.TextColumn("Código", disabled=True),
                            "produto": st.column_config.TextColumn("Produto", disabled=True, width="medium"),
                            "marca": st.column_config.TextColumn("Marca", disabled=True),
                            "estoque": st.column_config.NumberColumn("Estoque", disabled=True, format="%.0f und"),
                            f"venda({dias_alvo}D)": st.column_config.NumberColumn(f"Venda({dias_alvo}D)", disabled=True, format="%.0f und"),
                            "dias_estoque": st.column_config.NumberColumn("Dias Estoque", disabled=True, format="%.1f dias"),
                            "reposicao": st.column_config.NumberColumn("✏️ Reposição", format="%.0f und"),
                            "fornecedor": config_fornecedor,
                            "grupo": None, "subgrupo": None, "subgrupo1": None
                        },
                        use_container_width=True,
                        hide_index=True,
                        height=500,
                        key="editor_cobertura"
                    )
                    
                    # Processar itens desmarcados (remover da lista)
                    itens_desmarcados = df_editado[df_editado['incluir'] == False]['idproduto'].tolist()
                    if itens_desmarcados:
                        for item in itens_desmarcados:
                            if item not in itens_removidos:
                                itens_removidos.append(item)
                        st.session_state.itens_removidos = itens_removidos
                        st.rerun()
                    
                    # Botão de Envio
                    col_btn, _ = st.columns([1, 3])
                    with col_btn:
                        if st.button("📋 ENVIAR PEDIDOS", type="primary", use_container_width=True):
                            st.session_state['confirmar_envio_cobertura'] = True
                            st.rerun()
                    
                    # Pop-up de confirmação
                    if st.session_state.get('confirmar_envio_cobertura', False):
                        @st.dialog("Confirmar Envio de Pedidos")
                        def confirmar_envio_cobertura():
                            df_envio = df_editado[df_editado['incluir'] == True].copy()
                            
                            if df_envio.empty:
                                st.warning("Nenhum item para enviar.")
                                if st.button("Fechar", use_container_width=True):
                                    del st.session_state['confirmar_envio_cobertura']
                                    st.rerun()
                                return
                            
                            # Determinar fornecedores baseado no modo
                            if modo_envio == "Pré Definido":
                                fornecedores_destino = df_envio['fornecedor'].unique().tolist()
                            elif modo_envio == "Todos os Fornecedores":
                                fornecedores_destino = lista_fornecedores
                            elif modo_envio == "Fornecedor Único":
                                fornecedores_destino = [fornecedor_unico] if fornecedor_unico else []
                            else:
                                fornecedores_destino = fornecedores_selecionados
                            
                            if not fornecedores_destino:
                                st.warning("Selecione pelo menos um fornecedor.")
                                if st.button("Fechar", use_container_width=True):
                                    del st.session_state['confirmar_envio_cobertura']
                                    st.rerun()
                                return
                            
                            qtd_itens = len(df_envio)
                            qtd_fornecedores = len(fornecedores_destino)
                            
                            st.warning(f"📦 Deseja confirmar o envio de **{qtd_itens}** itens para **{qtd_fornecedores}** fornecedor(es)?")
                            st.info(f"📄 Fornecedores: {', '.join(fornecedores_destino[:3])}{'...' if len(fornecedores_destino) > 3 else ''}")
                            
                            col1, col2 = st.columns(2)
                            with col1:
                                if st.button("✅ Confirmar Envio", type="primary", use_container_width=True):
                                    cliente_nome = st.session_state.get('nome_usuario', 'Cliente')
                                    enviados = 0
                                    erros = []
                                    st.session_state['_idcob_counter'] = st.session_state.get('_idcob_counter', 0) + 1
                                    idcobertura = (int(time.time() * 1000) + st.session_state['_idcob_counter']) % MAX_INT_POSTGRES
                                    
                                    with st.spinner("Enviando pedidos..."):
                                        if modo_envio == "Todos os Fornecedores":
                                            for forn in fornecedores_destino:
                                                try:
                                                    df_forn = df_envio.copy()
                                                    marcas_forn = df_forn_db[df_forn_db['fornecedor'] == forn]['marca'].str.upper().tolist()
                                                    if marcas_forn and 'marca' in df_forn.columns:
                                                        df_forn = df_forn[df_forn['marca'].str.upper().isin(marcas_forn)]
                                                    
                                                    if not df_forn.empty:
                                                        df_forn['Qtd Compra'] = df_forn['reposicao']
                                                        numero = self.db.criar_pedido(cliente_nome, forn, df_forn, idcobertura)
                                                        enviados += 1
                                                        logger.info(f"Pedido {numero} enviado para {forn} com {len(df_forn)} itens")
                                                except Exception as e:
                                                    erros.append(forn)
                                                    logger.error(f"Erro ao enviar para {forn}: {e}")
                                        elif modo_envio == "Fornecedores Específicos":
                                            # Para cada item, envia para todos os fornecedores listados
                                            for forn in fornecedores_selecionados:
                                                try:
                                                    # Filtra itens que este fornecedor deve receber
                                                    # FIX: regex=False evita interpretar chars especiais como metacaracteres regex
                                                    df_forn = df_envio[df_envio['fornecedor'].str.contains(forn, na=False, regex=False)].copy()
                                                    
                                                    if not df_forn.empty:
                                                        df_forn['Qtd Compra'] = df_forn['reposicao']
                                                        numero = self.db.criar_pedido(cliente_nome, forn, df_forn, idcobertura)
                                                        enviados += 1
                                                        logger.info(f"Pedido {numero} enviado para {forn} com {len(df_forn)} itens")
                                                except Exception as e:
                                                    erros.append(forn)
                                                    logger.error(f"Erro ao enviar para {forn}: {e}")
                                        else:
                                            for forn in fornecedores_destino:
                                                try:
                                                    if modo_envio == "Pré Definido":
                                                        df_forn = df_envio[df_envio['fornecedor'] == forn].copy()
                                                    elif modo_envio == "Fornecedor Único":
                                                        df_forn = df_envio.copy()
                                                    else:
                                                        df_forn = df_envio.copy()
                                                        marcas_forn = df_forn_db[df_forn_db['fornecedor'] == forn]['marca'].str.upper().tolist()
                                                        if marcas_forn and 'marca' in df_forn.columns:
                                                            df_forn = df_forn[df_forn['marca'].str.upper().isin(marcas_forn)]
                                                    
                                                    if not df_forn.empty:
                                                        df_forn['Qtd Compra'] = df_forn['reposicao']
                                                        numero = self.db.criar_pedido(cliente_nome, forn, df_forn, idcobertura)
                                                        enviados += 1
                                                        logger.info(f"Pedido {numero} enviado para {forn} com {len(df_forn)} itens")
                                                except Exception as e:
                                                    erros.append(forn)
                                                    logger.error(f"Erro ao enviar para {forn}: {e}")
                                    
                                    if enviados:
                                        st.toast(f"✅ Envio concluído! {enviados} pedido(s) enviado(s) com sucesso!", icon="✅")
                                        st.session_state.itens_removidos = []
                                        st.session_state.df_analise_cache = pd.DataFrame()
                                        st.session_state.filtros_anteriores = None
                                        st.session_state.filtrar_sem_fornecedor = False
                                        st.session_state.cache_qtd_itens = {}  # FIX: invalidar cache de qtd itens
                                        del st.session_state['confirmar_envio_cobertura']
                                        st.rerun()
                                    if erros:
                                        st.error(f"Falha ao registrar pedido para: {', '.join(erros)}")
                            with col2:
                                if st.button("❌ Cancelar", type="secondary", use_container_width=True):
                                    del st.session_state['confirmar_envio_cobertura']
                                    st.rerun()
                        confirmar_envio_cobertura()
            
            elif filtros_anteriores is not None and 'df_analise_cache' in st.session_state and st.session_state.df_analise_cache.empty:
                st.warning("Nenhum item encontrado com os filtros selecionados.")

    def tela_analise_retorno(self):
        if 'estrategia_ativa' not in st.session_state:
            st.session_state['estrategia_ativa'] = 'menor_preco'
        
        st.markdown('<h1 style="color:black; font-weight:900;">Inteligência de Compra</h1>', unsafe_allow_html=True)
        
        # CSS para correção de cores e botões
        st.markdown("""
            <style>
            .black-text { color: #333333 !important; font-weight: 500; }
            .pedido-num-bold { color: #0047AB !important; font-weight: 900; }
            .header-title { color: #000000 !important; font-weight: 800; font-size: 1rem; }
            /* Botão PDF sem hover branco */
            .stDownloadButton button {
                background: linear-gradient(135deg, #0047AB 0%, #000000 150%) !important;
                color: white !important;
                border: none !important;
            }
            .stDownloadButton button:hover {
                background: linear-gradient(135deg, #0047AB 0%, #000000 150%) !important;
                color: white !important;
                opacity: 0.9 !important;
                transform: scale(1.02) !important;
            }
            </style>
        """, unsafe_allow_html=True)
        
        # Verificar se há solicitação aberta
        solicitacao_aberta = st.session_state.get('solicitacao_aberta')
        
        if solicitacao_aberta:
            # MODO DETALHE - Mostrar apenas a análise da solicitação
            self._renderizar_detalhes_solicitacao(solicitacao_aberta)
            return
        
        # MODO LISTA - Mostrar todas as solicitações
        st.divider()
        cliente_nome = st.session_state.get('nome_usuario', '')
        
        # Cache da lista de solicitações
        cache_key_lista = f'lista_solicitacoes_{cliente_nome}'
        if cache_key_lista not in st.session_state:
            st.session_state[cache_key_lista] = self.db.buscar_pedidos_respondidos(cliente_nome)
        df_solicitacoes = st.session_state[cache_key_lista]
        
        if df_solicitacoes.empty:
            st.info("Nenhuma cotação respondida disponível no momento.")
            return
        
        # Cache de contagem de itens para evitar múltiplas queries
        if 'cache_qtd_itens' not in st.session_state:
            st.session_state.cache_qtd_itens = {}
        
        # Lista de solicitações
        with st.container(border=True):
            c_num, c_forn, c_qtd, c_data, c_acao = st.columns([2, 3, 2, 2, 2])
            c_num.markdown('<div class="header-title">Solicitação</div>', unsafe_allow_html=True)
            c_forn.markdown('<div class="header-title">Fornecedores</div>', unsafe_allow_html=True)
            c_qtd.markdown('<div class="header-title">Qtd Itens</div>', unsafe_allow_html=True)
            c_data.markdown('<div class="header-title">Data</div>', unsafe_allow_html=True)
            c_acao.markdown('<div class="header-title" style="text-align: center;">Ação</div>', unsafe_allow_html=True)
            
            for _, row in df_solicitacoes.iterrows():
                grupo = str(row['grupo_pedido'])
                fornecedores = str(row['fornecedores'])
                data_str = pd.to_datetime(row['data_solicitacao']).strftime('%d/%m/%Y')
                
                # Usar cache para quantidade de itens
                if grupo not in st.session_state.cache_qtd_itens:
                    df_temp = self.db.buscar_detalhes_comparativo(grupo)
                    st.session_state.cache_qtd_itens[grupo] = len(df_temp['codigo_produto'].unique()) if not df_temp.empty else 0
                qtd_itens = st.session_state.cache_qtd_itens[grupo]
                
                # Criar lista de fornecedores
                fornecedores_texto = fornecedores[:30] + '...' if len(fornecedores) > 30 else fornecedores
                
                # Verificar se este pedido está aberto
                solicitacao_aberta = st.session_state.get('solicitacao_aberta')
                esta_aberto = (solicitacao_aberta == grupo)
                
                # Verificar se é novo ANTES de abrir
                eh_novo = self._pedido_eh_novo(grupo)
                
                # Estilo de fundo apenas para coluna de solicitação
                estilo_solicitacao = 'background-color: #E3F2FD; border-left: 4px solid #0047AB; padding: 8px; border-radius: 5px;' if esta_aberto else 'padding-top:8px;'
                
                col_n, col_f, col_q, col_d, col_a = st.columns([2, 3, 2, 2, 2])
                
                # Adicionar badge NOVO na lista
                badge_novo_lista = ' <span style="background:#FFC107;color:#000000;padding:2px 8px;border-radius:8px;font-size:0.7rem;font-weight:bold;margin-left:6px;">NOVO</span>' if eh_novo else ''
                col_n.markdown(f'''
                    <div style="{estilo_solicitacao}">
                        <div class="pedido-num-bold">#{grupo}{badge_novo_lista}</div>
                    </div>
                ''', unsafe_allow_html=True)
                
                # Fornecedores sem tooltip
                col_f.markdown(f'<div class="black-text" style="padding-top:8px;">{fornecedores_texto}</div>', unsafe_allow_html=True)
                
                # Quantidade de itens
                col_q.markdown(f'<div class="black-text" style="padding-top:8px;">{qtd_itens} itens</div>', unsafe_allow_html=True)
                col_d.markdown(f'<div class="black-text" style="padding-top:8px;">{data_str}</div>', unsafe_allow_html=True)
                
                with col_a:
                    if st.button("Analisar", key=f"sol_{grupo}", use_container_width=True):
                        st.session_state['solicitacao_aberta'] = grupo
                        self._marcar_pedido_visualizado(grupo)
                        # Remover notificações relacionadas ao idcobertura
                        try:
                            with self.db.get_connection() as conn:
                                cur = conn.cursor()
                                # Buscar todos os números de pedido deste idcobertura
                                cur.execute("""
                                    SELECT numero_pedido FROM pedidos_vendas WHERE idcobertura = %s
                                """, (grupo,))
                                numeros_pedidos = [row[0] for row in cur.fetchall()]
                                # Remover notificações de todos os pedidos deste grupo
                                for num_pedido in numeros_pedidos:
                                    notif_id = f"{num_pedido}_Enviado"
                                    self.db.remover_notificacao(st.session_state.nome_usuario, notif_id)
                        except Exception as e:
                            logger.error(f"Erro ao remover notificações do grupo {grupo}: {e}")
                        st.rerun()
    
    def _renderizar_detalhes_solicitacao(self, grupo):
        """Renderiza os detalhes de uma solicitação específica"""
        if 'solicitacao_aberta' in st.session_state:
            # Verificar se é novo e exibir badge
            eh_novo = self._pedido_eh_novo(grupo)
            badge_novo = ' <span style="background:#FFC107;color:#000000;padding:4px 12px;border-radius:12px;font-size:0.8rem;font-weight:bold;margin-left:10px;">NOVO</span>' if eh_novo else ''
            
            st.markdown(f'<h2 style="color:black; font-weight:900;">Análise Comparativa - #{grupo}{badge_novo}</h2>', unsafe_allow_html=True)
            
            # Botão voltar
            if st.button("← Voltar para Lista", type="secondary", use_container_width=False):
                del st.session_state['solicitacao_aberta']
                # Limpar cache da lista para recarregar
                cache_key_lista = f'lista_solicitacoes_{st.session_state.get("nome_usuario", "")}'
                if cache_key_lista in st.session_state:
                    del st.session_state[cache_key_lista]
                st.rerun()
            
            st.divider()
            
            # Cache dos detalhes para evitar recarregar a cada interação
            cache_key = f'detalhes_{grupo}'
            if cache_key not in st.session_state:
                st.session_state[cache_key] = self.db.buscar_detalhes_comparativo(grupo)
            
            df_detalhes = st.session_state[cache_key]
            if df_detalhes.empty:
                st.warning("Nenhum detalhe encontrado.")
                return
            
            # Preparar dados (frete é texto, não soma)
            df_detalhes['valor_total'] = (df_detalhes['quantidade'] * df_detalhes['valor_unitario'].fillna(0)) + \
                                          df_detalhes['impostos'].fillna(0)
            
            # Pivot de preços
            index_cols = ['codigo_produto', 'nome_produto', 'quantidade']
            df_pivot = df_detalhes.pivot_table(
                index=index_cols, 
                columns='fornecedor_nome', 
                values='valor_unitario', 
                aggfunc='min'
            ).reset_index()
            
            cols_fornecedores = [c for c in df_pivot.columns if c not in index_cols]
            df_pivot['Melhor Preço'] = df_pivot[cols_fornecedores].min(axis=1)
            df_pivot['Vencedor'] = df_pivot[cols_fornecedores].idxmin(axis=1)
            
            # Verificar se há múltiplos fornecedores
            tem_multiplos_fornecedores = len(cols_fornecedores) > 1
            
            # Calcular métricas de economia (sempre que houver múltiplos fornecedores)
            if tem_multiplos_fornecedores:
                st.markdown('<h3 style="color:#000000;">Resumo da Análise</h3>', unsafe_allow_html=True)
                
                # CSS para corrigir cores das métricas
                st.markdown("""
                    <style>
                    [data-testid="stMetricValue"] > div { color: #000000 !important; }
                    [data-testid="stMetricLabel"] > div { color: #000000 !important; font-weight: 600 !important; }
                    [data-testid="stMetricDelta"] > div { color: #000000 !important; }
                    [data-testid="stMetric"] label { color: #000000 !important; }
                    [data-testid="stMetric"] * { color: #000000 !important; }
                    </style>
                """, unsafe_allow_html=True)
                
                total_menor_preco = (df_pivot['quantidade'] * df_pivot['Melhor Preço']).sum()
                total_maior_preco = (df_pivot['quantidade'] * df_pivot[cols_fornecedores].max(axis=1)).sum()
                economia_potencial = total_maior_preco - total_menor_preco
                percentual_economia = (economia_potencial / total_maior_preco * 100) if total_maior_preco > 0 else 0
                
                # Resumo visual com métricas
                col_m1, col_m2, col_m3 = st.columns(3)
                with col_m1:
                    st.metric("Melhor Cenário", f"R$ {total_menor_preco:,.2f}", delta=None)
                with col_m2:
                    st.metric("Economia Potencial", f"R$ {economia_potencial:,.2f}", delta=f"-{percentual_economia:.1f}%", delta_color="inverse")
                with col_m3:
                    fornecedor_mais_barato = df_pivot['Vencedor'].mode()[0] if not df_pivot['Vencedor'].empty else "N/A"
                    qtd_itens_vencedor = (df_pivot['Vencedor'] == fornecedor_mais_barato).sum()
                    st.metric("Fornecedor Destaque", fornecedor_mais_barato, delta=f"{qtd_itens_vencedor} itens")
                
                st.divider()
                
                # Exibir comparativo de preços
                st.markdown(f'<h3 style="color:#000000;">Comparativo de Preços ({len(cols_fornecedores)} Fornecedores)</h3>', unsafe_allow_html=True)
                
                # Adicionar coluna de diferença percentual
                df_pivot['Dif. Máx (%)'] = ((df_pivot[cols_fornecedores].max(axis=1) - df_pivot['Melhor Preço']) / df_pivot['Melhor Preço'] * 100).round(1)
                
                def destacar_minimo(row):
                    estilos = ['' for _ in range(len(row))]
                    melhor = row['Melhor Preço']
                    for i, col_nome in enumerate(row.index):
                        if col_nome in cols_fornecedores:
                            if row[col_nome] == melhor and melhor > 0:
                                estilos[i] = 'background-color: #D4EDDA; color: #155724; font-weight: bold'
                            elif pd.notna(row[col_nome]) and row[col_nome] > melhor:
                                # Destaque vermelho para preços acima do melhor
                                estilos[i] = 'background-color: #F8D7DA; color: #721C24'
                    return estilos
                
                # Criar dicionário de formatação completo
                format_dict = {
                    'quantidade': '{:.0f}',
                    'Melhor Preço': '{:.2f}',
                    'Dif. Máx (%)': '{:.1f}'
                }
                # Adicionar formatação para colunas de fornecedores
                for col in cols_fornecedores:
                    format_dict[col] = '{:.2f}'
                
                st.dataframe(
                    df_pivot.style.apply(destacar_minimo, axis=1).format(format_dict),
                    use_container_width=True, hide_index=True
                )
                st.divider()
                
                # Estratégias de fechamento
                st.markdown('<h3 style="color:#000000;">Estratégia de Fechamento</h3>', unsafe_allow_html=True)
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    if st.button("▼ Menor Custo", type="primary" if st.session_state['estrategia_ativa'] == 'menor_preco' else "secondary", use_container_width=True):
                        st.session_state['estrategia_ativa'] = 'menor_preco'
                        st.rerun()
                
                with col2:
                    if st.button("👤 Fornecedor Único", type="primary" if st.session_state['estrategia_ativa'] == 'fornecedor_unico' else "secondary", use_container_width=True):
                        st.session_state['estrategia_ativa'] = 'fornecedor_unico'
                        st.rerun()
                
                with col3:
                    if st.button("⏱️ Menor Prazo", type="primary" if st.session_state['estrategia_ativa'] == 'menor_prazo' else "secondary", use_container_width=True):
                        st.session_state['estrategia_ativa'] = 'menor_prazo'
                        st.rerun()
                
                estrategia = st.session_state['estrategia_ativa']
                pedidos = {}
                
                if estrategia == 'menor_preco':
                    st.info("💡 **Menor Custo:** Distribui itens pelo menor valor unitário.")
                    for forn in df_pivot['Vencedor'].dropna().unique():
                        itens_forn = df_pivot[df_pivot['Vencedor'] == forn][index_cols + ['Melhor Preço']].copy()
                        itens_forn = itens_forn.rename(columns={'Melhor Preço': 'Valor Unitário'})
                        pedidos[forn] = itens_forn
                    
                elif estrategia == 'fornecedor_unico':
                    escolhido = st.selectbox("Selecione o Fornecedor:", cols_fornecedores)
                    if escolhido:
                        st.warning(f"⚠️ Comprando apenas itens de {escolhido}")
                        itens_forn = df_pivot[df_pivot[escolhido].notna()][index_cols + [escolhido]].copy()
                        itens_forn = itens_forn.rename(columns={escolhido: 'Valor Unitário'})
                        pedidos[escolhido] = itens_forn
                        
                        itens_faltantes = df_pivot[df_pivot[escolhido].isna()]
                        if not itens_faltantes.empty:
                            st.warning(f"⚠️ {len(itens_faltantes)} itens não vendidos por {escolhido}. Distribuindo para outros.")
                            for _, item_row in itens_faltantes.iterrows():
                                for outro_forn in cols_fornecedores:
                                    if outro_forn != escolhido and pd.notna(item_row[outro_forn]):
                                        if outro_forn not in pedidos:
                                            pedidos[outro_forn] = []
                                        pedidos[outro_forn].append({
                                            'codigo_produto': item_row['codigo_produto'],
                                            'nome_produto': item_row['nome_produto'],
                                            'quantidade': item_row['quantidade'],
                                            'Valor Unitário': item_row[outro_forn]
                                        })
                                        break
                
                elif estrategia == 'menor_prazo':
                    if 'prazo_entrega' in df_detalhes.columns:
                        st.success("🚀 **Menor Prazo:** Priorizando entrega mais rápida.")
                        df_detalhes['prazo_entrega'] = pd.to_datetime(df_detalhes['prazo_entrega'], errors='coerce')
                        
                        # Verificar se há prazos válidos
                        if df_detalhes['prazo_entrega'].notna().any():
                            df_prazo = df_detalhes.pivot_table(
                                index=index_cols,
                                columns='fornecedor_nome',
                                values='prazo_entrega',
                                aggfunc='min'
                            ).reset_index()
                            
                            # Verificar quais fornecedores estão no pivot de prazo
                            cols_prazo_disponiveis = [c for c in df_prazo.columns if c in cols_fornecedores]
                            
                            if cols_prazo_disponiveis:
                                # Calcular vencedor apenas para linhas com prazos válidos
                                df_prazo['Vencedor_Prazo'] = df_prazo[cols_prazo_disponiveis].idxmin(axis=1, skipna=True)
                                
                                for forn in cols_prazo_disponiveis:
                                    itens_forn = df_prazo[df_prazo['Vencedor_Prazo'] == forn]
                                    if not itens_forn.empty:
                                        merged = df_pivot.merge(itens_forn[index_cols], on=index_cols)
                                        if forn in merged.columns:
                                            merged = merged[index_cols + [forn]].rename(columns={forn: 'Valor Unitário'})
                                            pedidos[forn] = merged
                            else:
                                st.error("Nenhum fornecedor informou prazo de entrega.")
                        else:
                            st.error("Nenhum prazo de entrega válido foi informado pelos fornecedores.")
                    else:
                        st.error("Coluna 'Prazo' não encontrada.")
            else:
                # Apenas 1 fornecedor - gera pedido direto sem estratégia
                pedidos = {}
                fornecedor_unico = cols_fornecedores[0]
                itens_forn = df_pivot[index_cols + [fornecedor_unico]].copy()
                itens_forn = itens_forn.rename(columns={fornecedor_unico: 'Valor Unitário'})
                pedidos[fornecedor_unico] = itens_forn
            
            # Exibir pedidos
            if pedidos:
                    st.write("---")
                    
                    # CSS específico para Pedidos Gerados
                    st.markdown("""
                        <style>
                        /* Título Pedidos Gerados */
                        h3 { color: #000000 !important; }
                        /* Textos dentro das tabs de Pedidos Gerados */
                        div[data-testid="stTabs"] p:not(button p) { color: #000000 !important; }
                        div[data-testid="stTabs"] strong:not(button strong) { color: #000000 !important; }
                        div[data-testid="stTabs"] [data-testid="stMarkdownContainer"]:not(button *) { color: #000000 !important; }
                        /* Abas dos fornecedores em preto */
                        button[data-baseweb="tab"] { color: #000000 !important; }
                        </style>
                    """, unsafe_allow_html=True)
                    
                    st.markdown('<h3 style="color:#000000 !important;">Pedidos Gerados</h3>', unsafe_allow_html=True)
                    
                    # Informações estruturadas sobre os pedidos
                    st.markdown("""
                        <div style="background-color:#F8F9FA; padding:15px; border-radius:8px; border-left:4px solid #0047AB; margin-bottom:15px;">
                            <p style="color:#000000; margin:0; font-weight:600; font-size:0.95rem;">📋 Instruções de Envio</p>
                            <ul style="color:#333333; margin:8px 0 0 0; padding-left:20px; font-size:0.9rem;">
                                <li>Revise as quantidades e valores antes de confirmar</li>
                                <li>Edite a quantidade diretamente na tabela, se necessário</li>
                                <li>Baixe o PDF para registro antes do envio</li>
                                <li>Após o envio, o pedido será encaminhado ao fornecedor</li>
                            </ul>
                        </div>
                    """, unsafe_allow_html=True)
                    
                    # Usar tabs para múltiplos fornecedores
                    fornecedores_list = list(pedidos.keys())
                    tabs = st.tabs([f"{forn}" for forn in fornecedores_list])
                    
                    for idx, (fornecedor, df_pedido) in enumerate(pedidos.items()):
                        with tabs[idx]:
                            if isinstance(df_pedido, list):
                                df_pedido = pd.DataFrame(df_pedido)
                            if not df_pedido.empty:
                                total = (df_pedido['quantidade'] * df_pedido['Valor Unitário']).sum()
                                st.markdown(f'**Total: R$ {total:,.2f}** | {len(df_pedido)} itens', unsafe_allow_html=True)
                                st.write("")
                                
                                col_busca, _ = st.columns([3.5, 6.5])
                                with col_busca:
                                    busca = st.text_input(f"Buscar produto", key=f"busca_{fornecedor}")
                                
                                df_exibir = df_pedido.copy()
                                if busca:
                                    df_exibir = df_pedido[
                                        df_pedido['codigo_produto'].astype(str).str.contains(busca, case=False, na=False) |
                                        df_pedido['nome_produto'].astype(str).str.contains(busca, case=False, na=False)
                                    ]
                                
                                # Adicionar coluna Total
                                df_exibir['Total'] = df_exibir['quantidade'] * df_exibir['Valor Unitário']
                                
                                df_editado = st.data_editor(
                                    df_exibir,
                                    column_config={
                                        "codigo_produto": st.column_config.TextColumn("Código", disabled=True),
                                        "nome_produto": st.column_config.TextColumn("Produto", disabled=True, width="large"),
                                        "quantidade": st.column_config.NumberColumn("✏️ Quantidade", min_value=1, format="%d"),
                                        "Valor Unitário": st.column_config.NumberColumn("Valor Unit.", disabled=True, format="R$ %.2f"),
                                        "Total": st.column_config.NumberColumn("Total", disabled=True, format="R$ %.2f")
                                    },
                                    use_container_width=True,
                                    hide_index=True,
                                    key=f"editor_{fornecedor}"
                                )
                                
                                # Atualizar df_pedido com as edições
                                pedidos[fornecedor] = df_editado
                                
                                # Buscar e exibir observação do fornecedor
                                try:
                                    with self.db.get_connection() as conn:
                                        cur = conn.cursor()
                                        cur.execute("""
                                            SELECT observacao FROM pedidos_vendas 
                                            WHERE idcobertura = %s 
                                            AND LOWER(fornecedor_nome) = LOWER(%s)
                                            AND observacao IS NOT NULL AND observacao != ''
                                            LIMIT 1
                                        """, (grupo, fornecedor))
                                        result = cur.fetchone()
                                        if result:
                                            st.markdown(f"""
                                                <div style="background-color:#F8F9FA; padding:16px; border-radius:8px; border-left:4px solid #0047AB; margin:20px 0;">
                                                    <p style="color:#0047AB; margin:0 0 8px 0; font-weight:700; font-size:0.95rem;">Observações do Fornecedor</p>
                                                    <p style="color:#333333; margin:0; font-size:0.9rem; line-height:1.5;">{result[0]}</p>
                                                </div>
                                            """, unsafe_allow_html=True)
                                except Exception as e:
                                    logger.error(f"Erro ao buscar observação: {e}")
                                
                                # Botões PDF e Enviar
                                col_pdf, col_enviar = st.columns(2)
                                with col_pdf:
                                    pdf_bytes = self.gerar_pdf_pedido(fornecedor, df_editado)
                                    st.download_button(
                                        "📄 Baixar PDF",
                                        pdf_bytes,
                                        f"Pedido_{fornecedor}.pdf",
                                        "application/pdf",
                                        key=f"pdf_{fornecedor}",
                                        use_container_width=True,
                                        type="primary"
                                    )
                                with col_enviar:
                                    if st.button("✅ Enviar Pedido", key=f"enviar_{fornecedor}", type="primary", use_container_width=True):
                                        # Validar quantidade antes de enviar
                                        if df_editado['quantidade'].min() < 1:
                                            st.error("❌ Quantidade deve ser maior que zero")
                                        else:
                                            # Pop-up de confirmação
                                            st.session_state[f'confirmar_envio_{fornecedor}'] = True
                                            st.rerun()
                                
                                # Pop-up de confirmação
                                if st.session_state.get(f'confirmar_envio_{fornecedor}', False):
                                    @st.dialog("Confirmar Envio de Pedido")
                                    def confirmar():
                                        qtd_itens = len(df_editado)
                                        total = (df_editado['quantidade'] * df_editado['Valor Unitário']).sum()
                                        st.warning(f"📦 Deseja confirmar o envio do pedido para **{fornecedor}**?")
                                        st.info(f"📋 **{qtd_itens}** itens | Total: **R$ {total:,.2f}**")
                                        col1, col2 = st.columns(2)
                                        with col1:
                                            if st.button("✅ Confirmar", type="primary", use_container_width=True):
                                                cliente_nome = st.session_state.get('nome_usuario', 'Cliente')
                                                try:
                                                    df_envio = df_editado.copy()
                                                    df_envio['Qtd Compra'] = df_envio['quantidade']
                                                    
                                                    # Usar o mesmo idcobertura do grupo de orçamentos
                                                    grupo = st.session_state.get('solicitacao_aberta')
                                                    idcobertura_grupo = int(grupo) if grupo else (int(time.time() * 1000) + st.session_state.get('_idcob_counter', 0)) % MAX_INT_POSTGRES
                                                    
                                                    numero = self.db.criar_pedido(cliente_nome, fornecedor, df_envio, idcobertura_grupo)
                                                    
                                                    # Marcar como Confirmado usando context manager — FIX: evita vazamento de conexão e NameError no except
                                                    with self.db.get_connection() as conn:
                                                        cur = conn.cursor()
                                                        cur.execute("UPDATE pedidos_vendas SET status = %s WHERE numero_pedido = %s", (StatusPedido.CONFIRMADO, numero))
                                                        conn.commit()
                                                        cur.close()
                                                    
                                                    del st.session_state[f'confirmar_envio_{fornecedor}']
                                                    st.toast(f"✅ Pedido {numero} enviado para {fornecedor}!", icon="✅")
                                                    st.rerun()
                                                except Exception as e:
                                                    st.error(f"❌ Erro ao criar pedido para {fornecedor}. Tente novamente.")
                                                    logger.error(f"Erro ao criar pedido para {fornecedor}: {e}")
                                        with col2:
                                            if st.button("❌ Cancelar", type="secondary", use_container_width=True):
                                                del st.session_state[f'confirmar_envio_{fornecedor}']
                                                st.rerun()
                                    confirmar()

@st.dialog("Manual do Usuário", width="large")
def exibir_manual():
    # CSS para deixar todo o texto branco
    st.markdown("""
        <style>
        div[data-testid="stDialog"] * {
            color: #FFFFFF !important;
        }
        div[data-testid="stDialog"] h1,
        div[data-testid="stDialog"] h2,
        div[data-testid="stDialog"] h3,
        div[data-testid="stDialog"] p,
        div[data-testid="stDialog"] li,
        div[data-testid="stDialog"] strong,
        div[data-testid="stDialog"] code {
            color: #FFFFFF !important;
        }
        </style>
    """, unsafe_allow_html=True)
    
    perfil = st.session_state.get('perfil_usuario', 'CLIENTE')
    
    if perfil in ("ADM", "CLIENTE"):
        st.markdown("""
## 📘 Manual do Sistema - Área de Compras

Este manual detalha o funcionamento de cada etapa do processo de suprimentos.

---

### 1️⃣ Gerar Cobertura (Análise e Envio de Pedidos)
Identifica **o quê**, **quanto** e **onde** comprar, e envia diretamente aos fornecedores.

**Funcionalidades:**
* **Análise de Giro:** Calcula a velocidade de venda de cada item por filial
* **Sugestão Automática:** Baseada no estoque atual e lead time de entrega
* **Filtros Inteligentes:** Por filial, marca, grupo, subgrupo e produto
* **Dois Modos de Análise:**
  - **Sugestão de Compra:** Identifica itens com estoque baixo
  - **Análise de Sobra:** Identifica itens com excesso de estoque

**Como usar - Sugestão de Compra:**
1. Selecione os filtros desejados (filial, marca, grupo, etc.)
2. Escolha "Sugestão de Compra"
3. Configure os parâmetros (dias de cobertura alvo e estoque mínimo)
4. Clique em "GERAR ANÁLISE"
5. **Escolha o modo de envio:** (Não envia itens que não fazem parte do estoque do fornecedor)
   - **Pré Definido:** Usa o fornecedor já atribuído a cada item
   - **Todos os Fornecedores:** Envia para todos cadastrados
   - **Fornecedores Específicos:** Selecione quais receberão o pedido 
   - **Fornecedor Único:** Concentra todo o pedido em um fornecedor
6. **Edite a tabela:**
   - ✏️ **Reposição:** Ajuste a quantidade a comprar
   - ✏️ **Fornecedor:** Escolha/altere o fornecedor para cada item
   - **Incluir:** Desmarque itens que não deseja enviar
7. Use o botão **"↩️ Desfazer Última Remoção"** se excluir algo por engano
8. Clique em **"📋 ENVIAR PEDIDOS"** para criar os pedidos automaticamente

**Como usar - Análise de Sobra:**
1. Selecione os filtros desejados
2. Escolha "Análise de Sobra"
3. Configure o estoque máximo (dias)
4. Marque "Incluir não vendidos?" se desejar ver itens sem movimento
5. Clique em "GERAR ANÁLISE"
6. Baixe o PDF com o relatório de sobras

---

### 2️⃣ Inteligência de Compra (Análise Comparativa)
Compara cotações e identifica a melhor opção de compra.

**Funcionalidades:**
* **Visualização de Cotações:** Veja pedidos respondidos pelos fornecedores
* **Comparativo Automático:** Sistema destaca o melhor preço
* **Três Estratégias de Fechamento:**
  - **Menor Custo:** Escolhe o fornecedor mais barato por item
  - **Fornecedor Único:** Concentra compra em um fornecedor
  - **Menor Prazo:** Prioriza o prazo de entrega mais rápido
* **Geração de PDF:** Crie pedidos formatados para envio
* **Envio Direto:** Confirme pedidos diretamente pelo sistema

**Como usar:**
1. Acesse a tela "Inteligência de Compra"
2. Visualize as solicitações com cotações respondidas
3. Clique em "Analisar" na solicitação desejada
4. Escolha a estratégia de fechamento
5. Revise os pedidos gerados por fornecedor
6. Use a busca para localizar produtos específicos
7. Baixe os PDFs ou clique em "✅ Enviar Pedido" para confirmar

---

### 💡 Dicas Importantes
* **Produtos sem Marca:** O sistema atribui automaticamente fornecedor para itens sem marca cadastrada
* **Remoção de Itens:** Use o checkbox "Incluir" e desfaça com o botão de rollback
* **Edição Direta:** Todos os campos marcados com ✏️ são editáveis
* **Filtros:** Use filtros específicos para análises mais precisas
* **Modo de Envio:** Escolha o modo adequado conforme sua estratégia de compra
* **Rollback:** O botão desfaz remoções uma por uma (última removida primeiro)
* **Cache Otimizado:** Sistema carrega dados mais rápido com cache inteligente

---

### ⚠️ Observações
* O sistema cria pedidos separados automaticamente por fornecedor
* Após enviar, os pedidos ficam disponíveis para os fornecedores responderem
* Você pode acompanhar as respostas em "Inteligência de Compra"
* Ao trocar de aba, o sistema limpa automaticamente pedidos abertos
* Dados de catálogo (marcas, grupos, etc.) são atualizados a cada hora

---
*Você pode fechar esta janela clicando no 'X' ou fora da caixa.*
        """)
    
    elif perfil == "FORNECEDOR":
        st.markdown("""
## 📦 Manual do Fornecedor - Sistema de Pedidos

Bem-vindo ao portal de fornecedores da Rede Espafer!

---

### 🎯 Visão Geral
Como fornecedor, você tem acesso à área de **Pedidos**, onde pode:
* Visualizar pedidos enviados pelo Cliente
* Preencher valores unitários, impostos e frete
* Informar prazos de entrega
* Enviar sua cotação de volta

---

### 📋 Como Responder um Pedido

**Passo 1: Visualizar Pedidos**
* Na tela inicial, você verá todos os pedidos destinados a você
* Cada pedido mostra:
  - Número do pedido
  - Cliente solicitante
  - Status (Pendente/Enviado)
  - Data de criação

**Passo 2: Abrir Detalhes**
* Clique no botão "Ver / Responder" do pedido desejado
* O sistema abrirá os detalhes com todos os itens solicitados

**Passo 3: Preencher Informações**
Campos editáveis (marcados com ✏️):
* **Valor Unitário:** Preço por unidade do produto
* **Impostos:** Valor total de impostos (IPI, ICMS, etc.)
* **Frete:** Custo de transporte
* **Prazo de Entrega:** Data prevista para entrega

**Recursos úteis:**
* **Pesquisar Produto:** Localize itens por código ou nome
* **Preencher Todas as Datas:** Aplica a mesma data de entrega para todos os itens

**Passo 4: Revisar e Enviar**
* O sistema calcula automaticamente o **Total** do pedido
* Revise todos os valores preenchidos
* Clique em "✅ Confirmar Resposta" para enviar
* Você pode baixar um PDF do pedido antes de enviar

---

### ⚠️ Informações Importantes

**Status dos Pedidos:**
* 🟡 **Pendente:** Aguardando sua resposta
* 🟢 **Enviado:** Cotação já foi enviada ao cliente

**Dicas:**
* Preencha todos os campos para uma cotação completa
* Use a busca para localizar produtos rapidamente
* O botão "Preencher todas as datas" economiza tempo
* Você pode fechar e voltar ao pedido antes de confirmar
* Após confirmar, o status muda para "Enviado"

**Cálculo do Total:**
```
Total = (Quantidade × Valor Unitário) + Impostos + Frete
```

---

### 📄 Baixar PDF
Você pode gerar um PDF do pedido a qualquer momento:
* Útil para conferência interna
* Pode ser usado como comprovante
* Clique em "📄 Baixar PDF" antes ou depois de confirmar

---

### 🆘 Precisa de Ajuda?
Em caso de dúvidas sobre:
* Produtos específicos
* Quantidades solicitadas
* Prazos de entrega

Entre em contato com o departamento de compras da Rede Espafer.

---

*Você pode fechar esta janela clicando no 'X' ou fora da caixa.*
        """)
    
    col_fechar = st.columns(1)[0]
    if col_fechar.button("✅ Fechar Manual", type="primary", use_container_width=True, key="btn_fechar_manual_dialog"):
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
                    st.session_state.logado        = True
                    st.session_state.nome_usuario  = "Espafer"
                    st.session_state.perfil_usuario = "ADM"
                    st.session_state.tentativas_login = 0
                    logger.info("Login de emergência utilizado")
                    st.rerun()

                # --- 2. TENTATIVA VIA BANCO (com hash bcrypt) ---
                try:
                    db = DatabaseManager()
                    dados_usuario = db.validar_usuario(user_input, pass_input)

                    if dados_usuario:
                        st.session_state.logado         = True
                        st.session_state.nome_usuario   = dados_usuario[1]
                        st.session_state.perfil_usuario = dados_usuario[2]   # ADM / CLIENTE / FORNECEDOR
                        st.session_state.tentativas_login = 0
                        st.session_state.bloqueado_ate  = None
                        
                        # Limpar cache de análises e filtros ao fazer login
                        st.session_state.df_analise_cache = pd.DataFrame()
                        st.session_state.filtros_anteriores = None
                        st.session_state.itens_removidos = []
                        
                        # Define menu inicial conforme perfil
                        if dados_usuario[2] == "FORNECEDOR":
                            st.session_state.menu_ativo = "Orçamento"
                        else:
                            st.session_state.menu_ativo = "Gerar Cobertura"
                        st.success(f"Bem-vindo, {dados_usuario[1]}!")
                        logger.info(f"Login bem-sucedido: {user_input} — perfil: {dados_usuario[2]}")
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
    if not verificar_login():
        st.stop()

    app    = AppClientePrime()
    perfil = st.session_state.get('perfil_usuario', 'CLIENTE')
    app.render_sidebar()

    menu = st.session_state.menu_ativo

    # ── Roteamento por perfil ────────────────────────────────────
    if menu == "Orçamento":
        if perfil in ("ADM", "FORNECEDOR"):
            app.tela_pedidos_fornecedor()
        else:
            st.warning("Você não tem permissão para acessar esta área.")
    
    elif menu == "Pedidos":
        if perfil in ("ADM", "FORNECEDOR"):
            app.tela_pedidos_confirmados()
        else:
            st.warning("Você não tem permissão para acessar esta área.")

    elif menu in ("Gerar Cobertura", "Inteligência de Compra", "Meus Pedidos"):
        if perfil not in ("ADM", "CLIENTE"):
            st.warning("Você não tem permissão para acessar esta área.")
            st.stop()
        if menu == "Gerar Cobertura":
            app.tela_cobertura()
        elif menu == "Inteligência de Compra":
            app.tela_analise_retorno()
        elif menu == "Meus Pedidos":
            app.tela_visualizar_pedidos_cliente()

    # ── Rodapé da sidebar ────────────────────────────────────────
    st.sidebar.markdown("""
        <style>
            [data-testid="stSidebarContent"] { padding-bottom: 100px !important; }
            div.element-container:has(#ancora-rodape-limpo) + div {
                position: absolute !important; bottom: 30px !important;
                left: 0 !important; right: 0 !important; width: 85% !important;
                margin: auto !important; background-color: transparent !important;
                border: none !important; z-index: 9999;
            }
        </style>
        <div id="ancora-rodape-limpo"></div>
    """, unsafe_allow_html=True)

    with st.sidebar.container():
        # Notificações minimalistas
        notificacoes = app.db.buscar_notificacoes(st.session_state.nome_usuario, perfil)
        
        if notificacoes:
            st.markdown(f'<div style="color:#FFFFFF;font-size:0.75rem;opacity:0.7;margin-bottom:8px;">🔔 {len(notificacoes)} notificação(ões)</div>', unsafe_allow_html=True)
            
            for notif in notificacoes[:3]:
                col1, col2 = st.columns([20, 1])
                with col1:
                    st.markdown(f"""
                        <div style="background:rgba(0,0,0,0.2);padding:6px 10px;border-radius:6px;border-left:2px solid {notif['cor']};">
                            <span style="font-size:0.9rem;">{notif['icone']}</span>
                            <span style="color:#FFFFFF;font-size:0.75rem;margin-left:6px;opacity:0.9;">{notif['mensagem'][:50]}...</span>
                        </div>
                    """, unsafe_allow_html=True)
                
                with col2:
                    st.markdown("""
                        <style>
                        section[data-testid="stSidebar"] div[data-testid="stHorizontalBlock"] > div:last-child button {
                            background: none !important;
                            border: none !important;
                            color: #FFFFFF !important;
                            font-size: 1.2rem !important;
                            padding: 0 !important;
                            min-width: 20px !important;
                            opacity: 0.6 !important;
                            margin-top: -8px !important;
                        }
                        section[data-testid="stSidebar"] div[data-testid="stHorizontalBlock"] > div:last-child button:hover {
                            opacity: 1 !important;
                        }
                        </style>
                    """, unsafe_allow_html=True)
                    
                    if st.button("✕", key=f"notif_x_{notif['id']}"):
                        app.db.remover_notificacao(st.session_state.nome_usuario, notif['id'])
                        st.rerun()
            
            st.markdown("<div style='margin-bottom:10px;'></div>", unsafe_allow_html=True)
        
        if st.button("📖 Manual do Usuário", key="btn_manual_v6_final", use_container_width=True):
            st.session_state.show_manual = True

    if st.session_state.get('show_manual', False):
        exibir_manual()
        st.session_state.show_manual = False