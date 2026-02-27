import bcrypt
import streamlit as st
import pandas as pd
import psycopg2
from fpdf import FPDF
import datetime
import logging
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
# (Removido BLACKLIST_FILIAIS - não utilizado)

# ==================== FUNÇÕES AUXILIARES ====================
# (Removido validar_periodo_datas - não utilizado)

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

    def buscar_pedidos_fornecedor(self, nome_fornecedor=None):
        """Busca pedidos. Se nome_fornecedor informado, filtra por ele."""
        conn = None
        try:
            conn = self._get_connection()
            # Usa fornecedor_nome (texto) diretamente — mais robusto que depender de id na tabela fornecedores
            base = """
                SELECT pv.id,
                       pv.numero_pedido,
                       pv.cliente_nome,
                       pv.fornecedor_nome,
                       pv.status,
                       pv.data_criacao
                FROM pedidos_vendas pv
            """
            if nome_fornecedor:
                query = base + " WHERE LOWER(pv.fornecedor_nome) = LOWER(%s) ORDER BY pv.data_criacao DESC"
                return pd.read_sql(query, conn, params=(nome_fornecedor,))
            else:
                query = base + " ORDER BY pv.data_criacao DESC"
                return pd.read_sql(query, conn)
        except Exception as e:
            logger.error(f"Erro ao buscar pedidos: {e}")
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
                       COALESCE(frete, 0)            AS frete,
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

    def salvar_resposta_pedido(self, pedido_id, itens_df):
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
                    float(row.get('frete')          or 0),
                    str(row.get('prazo_entrega')    or ''),
                    int(row['id'])
                ))
            cur.execute("UPDATE pedidos_vendas SET status = 'Enviado' WHERE id = %s", (pedido_id,))
            conn.commit()
            cur.close()
            return True
        except Exception as e:
            logger.error(f"Erro ao salvar resposta pedido {pedido_id}: {e}")
            if conn: conn.rollback()
            return False
        finally:
            if conn: conn.close()

    def criar_pedido(self, cliente_nome, fornecedor_nome, itens_df):
        """Cria um pedido novo e insere os itens."""
        conn = None
        try:
            conn = self._get_connection()
            cur = conn.cursor()
    
            # 1. Gerar número do pedido (ex: PED-0001)
            cur.execute("""
                SELECT COALESCE(MAX(
                    CAST(NULLIF(regexp_replace(numero_pedido,'[^0-9]','','g'),'') AS INTEGER)
                ), 0) + 1 FROM pedidos_vendas
            """)
            proximo_numero = cur.fetchone()[0]
            numero_pedido = f"PED-{proximo_numero:04d}"
    
            # 2. Buscar o id_fornecedor na tabela fornecedores
            # Ajustado para usar 'id_fornecedor' conforme sua alteração de tabela anterior
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
    
            # 3. Verificar se a coluna fornecedor_nome existe (evita erro de transação)
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'pedidos_vendas' AND column_name = 'fornecedor_nome'
            """)
            tem_col_forn_nome = cur.fetchone() is not None
    
            # 4. Inserir o Cabeçalho do Pedido
            if tem_col_forn_nome:
                cur.execute("""
                    INSERT INTO public.pedidos_vendas
                        (numero_pedido, cliente_nome, fornecedor_nome, id_fornecedor, status, data_criacao)
                    VALUES (%s, %s, %s, %s, 'Pendente', CURRENT_TIMESTAMP)
                    RETURNING id
                """, (numero_pedido, cliente_nome, fornecedor_nome, id_fornecedor_bd))
            else:
                cur.execute("""
                    INSERT INTO public.pedidos_vendas
                        (numero_pedido, cliente_nome, id_fornecedor, status, data_criacao)
                    VALUES (%s, %s, %s, 'Pendente', CURRENT_TIMESTAMP)
                    RETURNING id
                """, (numero_pedido, cliente_nome, id_fornecedor_bd))
    
            pedido_id = cur.fetchone()[0]
    
            # 5. Inserir os Itens do Pedido
            for _, row in itens_df.iterrows():
                # Tenta pegar a quantidade de várias colunas possíveis
                qtd = row.get('Qtd Compra') or row.get('quantidade') or row.get('Quantidade') or 0
                
                # Limpeza de dados básica
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
            logger.info(f"Sucesso: Pedido {numero_pedido} criado para {fornecedor_nome}")
            return numero_pedido
    
        except Exception as e:
            if conn:
                conn.rollback() # CRITICAL: Destrava o banco para o próximo fornecedor do loop
            logger.error(f"Erro fatal ao criar pedido para {fornecedor_nome}: {e}")
            raise e 
        finally:
            if conn:
                conn.close()

    def buscar_filiais(self):
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

    def buscar_pedidos_respondidos(self, cliente_nome=None):
        """Busca pedidos com status Enviado agrupados por número base."""
        conn = None
        try:
            conn = self._get_connection()
            query = """
                SELECT 
                    REGEXP_REPLACE(numero_pedido, '-\\d+$', '') as grupo_pedido,
                    COUNT(DISTINCT id) as qtd_fornecedores,
                    MIN(data_criacao) as data_solicitacao,
                    STRING_AGG(DISTINCT fornecedor_nome, ', ') as fornecedores,
                    STRING_AGG(DISTINCT numero_pedido, ', ' ORDER BY numero_pedido) as numeros_pedidos
                FROM pedidos_vendas
                WHERE status = 'Enviado'
            """
            if cliente_nome:
                query += " AND LOWER(cliente_nome) = LOWER(%s)"
            query += " GROUP BY grupo_pedido ORDER BY MIN(data_criacao) DESC"
            
            if cliente_nome:
                return pd.read_sql(query, conn, params=(cliente_nome,))
            else:
                return pd.read_sql(query, conn)
        except Exception as e:
            logger.error(f"Erro ao buscar pedidos respondidos: {e}")
            return pd.DataFrame()
        finally:
            if conn: conn.close()

    def buscar_detalhes_comparativo(self, grupo_pedido):
        """Busca detalhes de todos os fornecedores de um grupo de pedidos."""
        conn = None
        try:
            conn = self._get_connection()
            query = """
                SELECT 
                    pv.fornecedor_nome,
                    pi.codigo_produto,
                    pi.nome_produto,
                    pi.quantidade,
                    pi.valor_unitario,
                    pi.impostos,
                    pi.frete,
                    pi.prazo_entrega
                FROM pedidos_vendas pv
                JOIN pedidos_itens pi ON pv.id = pi.pedido_id
                WHERE REGEXP_REPLACE(pv.numero_pedido, '-\\d+$', '') = %s
                  AND pv.status = 'Enviado'
                  AND pi.valor_unitario > 0
                ORDER BY pi.codigo_produto, pv.fornecedor_nome
            """
            return pd.read_sql(query, conn, params=(grupo_pedido,))
        except Exception as e:
            logger.error(f"Erro ao buscar detalhes comparativo: {e}")
            return pd.DataFrame()
        finally:
            if conn: conn.close()

    def buscar_fornecedores(self):
        """
        Retorna apenas usuários ativos com perfil FORNECEDOR em usuarios_sistema,
        cruzando com a tabela fornecedores pelo nome para obter a marca associada.
        Colunas retornadas: fornecedor (nome do usuário), marca
        """
        if not self.creds: return pd.DataFrame()
        conn = None
        try:
            conn = self._get_connection()
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
        """Verifica quais produtos da lista o fornecedor possui (baseado na marca)"""
        if not self.creds or not lista_produtos: return set(), set()
        conn = None
        try:
            conn = self._get_connection()
            query = """
                SELECT DISTINCT TRIM(UPPER(f.marca)) as marca
                FROM fornecedores f
                WHERE LOWER(TRIM(f.fornecedor)) = LOWER(TRIM(%s))
                  AND f.marca IS NOT NULL AND f.marca != ''
            """
            df_marcas = pd.read_sql(query, conn, params=(fornecedor_nome,))
            marcas_fornecedor = set(df_marcas['marca'].tolist()) if not df_marcas.empty else set()
            
            if not marcas_fornecedor:
                return set(), set(lista_produtos)
            
            possui = set()
            nao_possui = set()
            
            for prod_id in lista_produtos:
                query_prod = """
                    SELECT TRIM(UPPER(marca)) as marca
                    FROM cad_produto
                    WHERE TRIM(CAST(codacessog AS TEXT)) = %s
                    LIMIT 1
                """
                df_prod = pd.read_sql(query_prod, conn, params=(str(prod_id).strip(),))
                if not df_prod.empty:
                    marca_prod = df_prod['marca'].iloc[0]
                    if marca_prod in marcas_fornecedor:
                        possui.add(prod_id)
                    else:
                        nao_possui.add(prod_id)
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

    def inicializar_estado(self):
        perfil = st.session_state.get('perfil_usuario', 'CLIENTE')
        menu_default = "Pedidos" if perfil == "FORNECEDOR" else "Gerar Cobertura"
        if 'menu_ativo' not in st.session_state: st.session_state.menu_ativo = menu_default
        if 'dados_orcamento' not in st.session_state: st.session_state.dados_orcamento = None
        if 'modo_analise_atual' not in st.session_state: st.session_state.modo_analise_atual = "COMPRA"
        if 'tentativas_login' not in st.session_state: st.session_state.tentativas_login = 0
        if 'perfil_usuario' not in st.session_state: st.session_state.perfil_usuario = "CLIENTE"
        if 'db_fornecedores' not in st.session_state: st.session_state.db_fornecedores = {}

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
                    
                .pedido-container-row {
                border: 1px solid #E0E0E0; /* Cor da borda */
                border-radius: 10px;        /* Bordas arredondadas */
                padding: 15px;             /* Espaçamento interno */
                margin-bottom: 12px;       /* Espaçamento entre um pedido e outro */
                background-color: #FFFFFF; /* Fundo branco */
                transition: transform 0.2s, box-shadow 0.2s;
            }
        
            .pedido-container-row:hover {
                border-color: #0047AB;     /* Borda azul ao passar o mouse */
                box-shadow: 0 4px 8px rgba(0,0,0,0.05);
            }
                    
            </style>
        """, unsafe_allow_html=True)

    def render_sidebar(self):
        perfil = st.session_state.get('perfil_usuario', 'CLIENTE')
        with st.sidebar:
            # Logo e Usuário no topo
            st.markdown(
                '<div style="padding:10px 0px;">' 
                '<h1 style="color:#0047AB !important; font-weight:900; margin-bottom: 8px; text-align: center;">ESPAFER</h1>',
                unsafe_allow_html=True
            )
            
            # Usuário e Logout logo abaixo da logo
            perfil_label = {"ADM": "🔑", "CLIENTE": "🏪", "FORNECEDOR": "🚚"}.get(perfil, "👤")
            col_user, col_logout = st.columns([3.5, 1])
            with col_user:
                st.markdown(
                    f'<div style="color: #FFFFFF; font-size: 0.95rem; padding-top: 6px; font-weight: 500;">{perfil_label} {st.session_state.nome_usuario}</div>',
                    unsafe_allow_html=True
                )
            with col_logout:
                if st.button("➜", key="btn_logout_top", help="Sair"):
                    st.session_state.logado = False
                    st.rerun()
            
            st.markdown("---")

            # ── MENUS POR PERFIL ──────────────────────────────────────
            if perfil in ("ADM", "CLIENTE"):
                with st.expander("PEDIDO DE COMPRA", expanded=True):
                    opcoes = ["Gerar Cobertura", "Gerar Orçamento", "Inteligência de Compra"]
                    for opt in opcoes:
                        tipo = "primary" if st.session_state.menu_ativo == opt else "secondary"
                        if st.button(opt, key=f"sub_{opt}", type=tipo, use_container_width=True):
                            st.session_state.menu_ativo = opt
                            st.rerun()

            if perfil in ("ADM", "FORNECEDOR"):
                with st.expander("FORNECEDOR", expanded=(perfil == "FORNECEDOR")):
                    opcoes_forn = ["Orçamento", "Pedidos"]
                    for opt in opcoes_forn:
                        tipo = "primary" if st.session_state.menu_ativo == opt else "secondary"
                        if st.button(opt, key=f"sub_{opt}", type=tipo, use_container_width=True):
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

    def gerar_pdf_sobra(self, df, dias_corte, dias_alvo):
        df = df.sort_values(by='vol_estoque', ascending=False)
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
            pdf.cell(30, 7, f"{row['vol_estoque']:.0f} und", 'B', 0, 'R', True)
            
            pdf.set_font('Arial', '', 7)
            pdf.cell(33, 7, f"{row['venda_periodo']:.1f}", 'B', 1, 'R', True)

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
                with st.spinner("Registrando pedidos..."):
                    for forn, linhas in pedidos_por_forn.items():
                        df_forn_itens = pd.DataFrame(linhas)
                        try:
                            numero = self.db.criar_pedido(cliente_nome, forn, df_forn_itens)
                            enviados += 1
                            logger.info(f"Pedido {numero} → {forn} ({len(linhas)} itens)")
                        except Exception as e:
                            erros.append(forn)
                            st.error(f"❌ Erro ao registrar pedido para **{forn}**: `{e}`")

                if enviados:
                    st.success(
                        f"✅ {enviados} pedido(s) registrado(s). "
                        "Cada fornecedor verá apenas os itens da sua marca."
                    )
                    st.session_state.dados_orcamento = None
                if erros:
                    st.error(f"Falha ao registrar pedido para: {', '.join(erros)}")

    def tela_pedidos_confirmados(self):
        """Tela para fornecedor visualizar pedidos confirmados pelo cliente"""
        perfil = st.session_state.get('perfil', 'FORNECEDOR')
        nome_usuario = st.session_state.get('nome_usuario', '')

        st.markdown('<h1 style="color:black; font-weight:900;">📦 Pedidos Confirmados</h1>', unsafe_allow_html=True)

        # CSS para correção de cores
        st.markdown("""
            <style>
            .black-text { color: #333333 !important; font-weight: 500; }
            .pedido-num-bold { color: #0047AB !important; font-weight: 900; }
            .header-title { color: #000000 !important; font-weight: 800; font-size: 1rem; }
            </style>
        """, unsafe_allow_html=True)

        # Buscar pedidos confirmados do session_state
        pedidos_confirmados = st.session_state.get('pedidos_confirmados', {})
        
        if not pedidos_confirmados:
            st.info("Nenhum pedido confirmado disponível no momento.")
            return

        # Filtrar por fornecedor se não for ADM
        if perfil == "FORNECEDOR":
            pedidos_confirmados = {k: v for k, v in pedidos_confirmados.items() if k == nome_usuario}

        if not pedidos_confirmados:
            st.info("Nenhum pedido confirmado para você no momento.")
            return

        # Lista de pedidos confirmados
        with st.container(border=True):
            c_forn, c_qtd, c_total, c_acao = st.columns([3, 2, 2, 2])
            c_forn.markdown('<div class="header-title">Fornecedor</div>', unsafe_allow_html=True)
            c_qtd.markdown('<div class="header-title">Qtd Itens</div>', unsafe_allow_html=True)
            c_total.markdown('<div class="header-title">Total</div>', unsafe_allow_html=True)
            c_acao.markdown('<div class="header-title">Ação</div>', unsafe_allow_html=True)
            st.markdown("<div style='margin-bottom: 10px;'></div>", unsafe_allow_html=True)

            for fornecedor, itens in pedidos_confirmados.items():
                df_pedido = pd.DataFrame(itens)
                qtd_itens = len(df_pedido)
                total = (df_pedido['quantidade'] * df_pedido['Valor Unitário']).sum()
                
                col_f, col_q, col_t, col_a = st.columns([3, 2, 2, 2])
                col_f.markdown(f'<div class="pedido-num-bold" style="padding-top:8px;">{fornecedor}</div>', unsafe_allow_html=True)
                col_q.markdown(f'<div class="black-text" style="padding-top:8px;">{qtd_itens} itens</div>', unsafe_allow_html=True)
                col_t.markdown(f'<div class="black-text" style="padding-top:8px;">R$ {total:,.2f}</div>', unsafe_allow_html=True)
                
                pedido_aberto = st.session_state.get('pedido_confirmado_aberto')
                texto_botao = "✖ Fechar" if pedido_aberto == fornecedor else "Ver Detalhes"
                
                if col_a.button(texto_botao, key=f"ver_conf_{fornecedor}", use_container_width=True):
                    if pedido_aberto == fornecedor:
                        del st.session_state['pedido_confirmado_aberto']
                    else:
                        st.session_state['pedido_confirmado_aberto'] = fornecedor
                    st.rerun()

        # Detalhe do pedido confirmado
        if 'pedido_confirmado_aberto' in st.session_state:
            fornecedor = st.session_state['pedido_confirmado_aberto']
            st.divider()
            st.markdown(f'<h2 style="color:black; font-weight:900;">Detalhes do Pedido - {fornecedor}</h2>', unsafe_allow_html=True)
            
            df_pedido = pd.DataFrame(pedidos_confirmados[fornecedor])
            st.dataframe(df_pedido, use_container_width=True, hide_index=True)
            
            total = (df_pedido['quantidade'] * df_pedido['Valor Unitário']).sum()
            st.markdown(f'<div style="text-align:right; font-size:1.2rem; font-weight:900; color:#0047AB;">Total: R$ {total:,.2f}</div>', unsafe_allow_html=True)
            
            col_pdf, col_fechar = st.columns(2)
            with col_pdf:
                pdf_bytes = self.gerar_pdf_pedido(fornecedor, df_pedido)
                st.download_button("📄 Baixar PDF", pdf_bytes, f"Pedido_Confirmado_{fornecedor}.pdf", "application/pdf", use_container_width=True)
            with col_fechar:
                if st.button("✖ Fechar", use_container_width=True, key=f"close_conf_{fornecedor}"):
                    del st.session_state['pedido_confirmado_aberto']
                    st.rerun()

    def tela_pedidos_fornecedor(self):
        perfil = st.session_state.get('perfil', 'FORNECEDOR')
        nome_usuario = st.session_state.get('nome_usuario', '')

        st.markdown('<h1 style="color:black; font-weight:900;">📋 Orçamento</h1>', unsafe_allow_html=True)

        # 1. BUSCA DE DADOS
        if perfil == "ADM":
            df_pedidos = self.db.buscar_pedidos_fornecedor()
        else:
            df_pedidos = self.db.buscar_pedidos_fornecedor(nome_usuario)

        if df_pedidos.empty:
            st.info("Nenhum pedido disponível no momento.")
            return

        # 2. CSS PARA CORREÇÃO DE CORES (Evita texto branco)
        st.markdown("""
            <style>
            .black-text { color: #333333 !important; font-weight: 500; }
            .pedido-num-bold { color: #0047AB !important; font-weight: 900; }
            .header-title { color: #000000 !important; font-weight: 800; font-size: 1rem; }
            /* Ajuste do botão de PDF para não sumir no hover */
            .stDownloadButton button:hover {
                background-color: #0047AB !important;
                color: white !important;
            }
            </style>
        """, unsafe_allow_html=True)

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
            for _, row in df_pedidos.iterrows():
                pedido_id  = int(row['id'])
                num        = str(row['numero_pedido'])
                cliente    = str(row['cliente_nome'])
                status     = str(row['status'])
                data_str   = pd.to_datetime(row['data_criacao']).strftime('%d/%m/%Y') if row['data_criacao'] else '-'
                
                badge_cls = "badge-enviado" if status == "Enviado" else "badge-pendente"
                badge_bg = "#D4EDDA" if status == "Enviado" else "#FFF3CD"
                badge_txt = "#155724" if status == "Enviado" else "#856404"

                col_n, col_c, col_s, col_d, col_a = st.columns([1.5, 2.5, 1.5, 1.8, 1.5])
                
                col_n.markdown(f'<div class="pedido-num-bold" style="padding-top:8px;">#{num}</div>', unsafe_allow_html=True)
                col_c.markdown(f'<div class="black-text" style="padding-top:8px;">{cliente}</div>', unsafe_allow_html=True)
                col_s.markdown(f'''
                    <div style="background:{badge_bg}; color:{badge_txt}; padding:4px 8px; 
                    border-radius:12px; font-size:0.8rem; font-weight:bold; text-align:center; margin-top:8px;">
                        {status}
                    </div>
                ''', unsafe_allow_html=True)
                col_d.markdown(f'<div class="black-text" style="padding-top:8px;">{data_str}</div>', unsafe_allow_html=True)
                
                pedido_aberto = st.session_state.get('pedido_aberto')
                texto_botao = "✖ Fechar" if pedido_aberto == pedido_id else "Ver / Responder"
                
                if col_a.button(texto_botao, key=f"ver_{pedido_id}", use_container_width=True):
                    if pedido_aberto == pedido_id:
                        del st.session_state['pedido_aberto']
                        if 'pedido_num' in st.session_state:
                            del st.session_state['pedido_num']
                    else:
                        st.session_state['pedido_aberto'] = pedido_id
                        st.session_state['pedido_num']    = num
                    st.rerun()

        # 5. DETALHE DO PEDIDO (Abre abaixo da lista ao clicar)
        if 'pedido_aberto' in st.session_state:
            pedido_id = st.session_state['pedido_aberto']
            num       = st.session_state.get('pedido_num', pedido_id)
            
            st.divider()
            # Título com F-String corrigida
            st.markdown(f'<h2 style="color:black; font-weight:900;">Detalhes do Pedido #{num}</h2>', unsafe_allow_html=True)

            df_itens = self.db.buscar_itens_pedido(pedido_id)
            if df_itens.empty:
                st.warning("Nenhum item encontrado para este pedido.")
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
                    frete_global = st.number_input("Preencher Frete", min_value=0.0, step=0.01, key=f"frete_global_{pedido_id}")
                
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
                if frete_global > 0:
                    df_itens_filtrado['frete'] = frete_global

                df_edit = st.data_editor(
                    df_itens_filtrado,
                    column_config={
                        "codigo_produto": st.column_config.TextColumn("Codigo", disabled=True),
                        "nome_produto":   st.column_config.TextColumn("Produto", disabled=True, width="large"),
                        "quantidade":     st.column_config.NumberColumn("Qtd",  disabled=True),
                        "valor_unitario": st.column_config.NumberColumn("✏️ Vl. Unit.", format="R$ %.2f", min_value=0.0),
                        "impostos":       st.column_config.NumberColumn("✏️ Impostos", format="R$ %.2f", min_value=0.0),
                        "frete":          st.column_config.NumberColumn("✏️ Frete",    format="R$ %.2f", min_value=0.0),
                        "prazo_entrega":  st.column_config.DateColumn("✏️ Prazo", format="DD/MM/YYYY"),
                    },
                    hide_index=True, use_container_width=True, key=f"editor_{pedido_id}"
                )

                # Cálculo de Total
                df_edit['_total'] = (df_edit['quantidade'] * df_edit['valor_unitario'].fillna(0)) + \
                                     df_edit['impostos'].fillna(0) + df_edit['frete'].fillna(0)
                total_geral = df_edit['_total'].sum()

                st.markdown(f'<div style="text-align:right; font-size:1.2rem; font-weight:900; color:#0047AB;">Total: R$ {total_geral:,.2f}</div>', unsafe_allow_html=True)
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
                        if self.db.salvar_resposta_pedido(pedido_id, df_edit):
                            st.success("Resposta enviada!")
                            del st.session_state['pedido_aberto']
                            st.rerun()

                with col_fechar:
                    if st.button("✖ Fechar", use_container_width=True, key=f"close_{pedido_id}"):
                        del st.session_state['pedido_aberto']
                        st.rerun()

    def tela_cobertura(self):
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
                
                # Renomear colunas
                df_processado = df_processado.rename(columns={
                    'vol_estoque': 'estoque',
                    'venda_periodo': f'venda({dias_alvo}D)'
                })

                # 2. DEFINIÇÃO DE COLUNAS
                lista_base = ['filial', 'idproduto', 'produto', 'marca', 'estoque', f'venda({dias_alvo}D)', 'dias_estoque', 'reposicao']
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

                    formatos = {'estoque': '{:.0f} und', f'venda({dias_alvo}D)': '{:.0f} und', 'dias_estoque': '{:.1f} dias'}
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
                        pdf_bytes = self.gerar_pdf_sobra(df_final, dias_corte, dias_alvo)
                        st.download_button(
                            label="📄 Baixar PDF de Sobras",
                            data=pdf_bytes,
                            file_name=f"Sobras_{pd.Timestamp.now().strftime('%d%m')}.pdf",
                            mime="application/pdf",
                            type="primary",
                            use_container_width=True
                        )
                    
                    else:
                        if st.button("➕ Adicionar TODOS ao Orçamento", type="primary", use_container_width=True):
                            df_orc = df_final.copy()
                            if 'reposicao' in df_orc.columns:
                                df_orc['Qtd Compra'] = df_orc['reposicao']
                            
                            # Correção aqui: adaptando os nomes das colunas para os novos filtros
                            cols_para_orc = ['idproduto', 'produto', 'estoque', 'marca', 'grupo', 'subgrupo', f'venda({dias_alvo}D)', 'dias_estoque', 'Qtd Compra']
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
        if 'estrategia_ativa' not in st.session_state:
            st.session_state['estrategia_ativa'] = 'menor_preco'
        
        st.markdown('<h1 style="color:black; font-weight:900;">💡 Inteligência de Compra</h1>', unsafe_allow_html=True)
        
        # CSS para correção de cores e botões
        st.markdown("""
            <style>
            .black-text { color: #333333 !important; font-weight: 500; }
            .pedido-num-bold { color: #0047AB !important; font-weight: 900; }
            .header-title { color: #000000 !important; font-weight: 800; font-size: 1rem; }
            h3 { color: #000000 !important; }
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
            /* Tooltip customizado */
            .tooltip-container {
                position: relative;
                display: inline-block;
                cursor: help;
            }
            .tooltip-container .tooltip-text {
                visibility: hidden;
                width: 300px;
                background-color: #0047AB;
                color: #fff;
                text-align: left;
                border-radius: 8px;
                padding: 10px;
                position: absolute;
                z-index: 1000;
                bottom: 125%;
                left: 50%;
                transform: translateX(-50%);
                opacity: 0;
                transition: opacity 0.3s;
                font-size: 0.85rem;
                box-shadow: 0 4px 8px rgba(0,0,0,0.3);
                max-height: 300px;
                overflow-y: auto;
            }
            .tooltip-container .tooltip-text::after {
                content: "";
                position: absolute;
                top: 100%;
                left: 50%;
                margin-left: -5px;
                border-width: 5px;
                border-style: solid;
                border-color: #0047AB transparent transparent transparent;
            }
            .tooltip-container:hover .tooltip-text {
                visibility: visible;
                opacity: 1;
            }
            /* Ajuste para tooltip na primeira coluna */
            .tooltip-container.tooltip-left .tooltip-text {
                left: 0;
                transform: translateX(0);
            }
            .tooltip-container.tooltip-left .tooltip-text::after {
                left: 20px;
            }
            </style>
        """, unsafe_allow_html=True)
        
        # Buscar solicitações respondidas
        cliente_nome = st.session_state.get('nome_usuario', '')
        df_solicitacoes = self.db.buscar_pedidos_respondidos(cliente_nome)
        
        if df_solicitacoes.empty:
            st.info("Nenhuma cotação respondida disponível no momento.")
            return
        
        # Lista de solicitações
        with st.container(border=True):
            c_num, c_forn, c_qtd, c_data, c_acao = st.columns([2, 3, 2, 2, 2])
            c_num.markdown('<div class="header-title">Solicitação</div>', unsafe_allow_html=True)
            c_forn.markdown('<div class="header-title">Fornecedores</div>', unsafe_allow_html=True)
            c_qtd.markdown('<div class="header-title">Qtd Itens</div>', unsafe_allow_html=True)
            c_data.markdown('<div class="header-title">Data</div>', unsafe_allow_html=True)
            c_acao.markdown('<div class="header-title">Ação</div>', unsafe_allow_html=True)
            
            for _, row in df_solicitacoes.iterrows():
                grupo = str(row['grupo_pedido'])
                qtd_forn = int(row['qtd_fornecedores'])
                fornecedores = str(row['fornecedores'])
                data_str = pd.to_datetime(row['data_solicitacao']).strftime('%d/%m/%Y')
                
                # Buscar quantidade total de itens
                df_temp = self.db.buscar_detalhes_comparativo(grupo)
                qtd_itens = len(df_temp['codigo_produto'].unique()) if not df_temp.empty else 0
                
                # Criar lista de números de pedidos para tooltip
                numeros_pedidos = str(row.get('numeros_pedidos', grupo))
                lista_pedidos = "<br>".join([f"• {num}" for num in numeros_pedidos.split(", ")])
                
                # Criar lista de fornecedores para tooltip
                lista_fornecedores_completa = "<br>".join([f"• {f}" for f in fornecedores.split(", ")])
                
                col_n, col_f, col_q, col_d, col_a = st.columns([2, 3, 2, 2, 2])
                
                # Solicitação com tooltip mostrando números dos pedidos
                col_n.markdown(f'''
                    <div class="tooltip-container tooltip-left" style="padding-top:8px;">
                        <div class="pedido-num-bold">#{grupo}</div>
                        <span class="tooltip-text">
                            <strong>Números dos Pedidos:</strong><br>
                            {lista_pedidos}
                        </span>
                    </div>
                ''', unsafe_allow_html=True)
                
                # Fornecedores com tooltip
                col_f.markdown(f'''
                    <div class="tooltip-container" style="padding-top:8px;">
                        <div class="black-text" style="font-size:0.85rem;">{fornecedores[:30]}...</div>
                        <span class="tooltip-text">
                            <strong>Fornecedores:</strong><br>
                            {lista_fornecedores_completa}
                        </span>
                    </div>
                ''', unsafe_allow_html=True)
                
                # Quantidade de itens sem tooltip
                col_q.markdown(f'<div class="black-text" style="padding-top:8px;">{qtd_itens} itens</div>', unsafe_allow_html=True)
                col_d.markdown(f'<div class="black-text" style="padding-top:8px;">{data_str}</div>', unsafe_allow_html=True)
                
                solicitacao_aberta = st.session_state.get('solicitacao_aberta')
                texto_botao = "✖ Fechar" if solicitacao_aberta == grupo else "Analisar"
                
                if col_a.button(texto_botao, key=f"sol_{grupo}", use_container_width=True):
                    if solicitacao_aberta == grupo:
                        del st.session_state['solicitacao_aberta']
                    else:
                        st.session_state['solicitacao_aberta'] = grupo
                    st.rerun()
        
        # Detalhes da solicitação
        if 'solicitacao_aberta' in st.session_state:
            grupo = st.session_state['solicitacao_aberta']
            st.divider()
            st.markdown(f'<h2 style="color:black; font-weight:900;">Análise Comparativa - #{grupo}</h2>', unsafe_allow_html=True)
            
            df_detalhes = self.db.buscar_detalhes_comparativo(grupo)
            if df_detalhes.empty:
                st.warning("Nenhum detalhe encontrado.")
                return
            
            # Preparar dados
            df_detalhes['valor_total'] = (df_detalhes['quantidade'] * df_detalhes['valor_unitario']) + \
                                          df_detalhes['impostos'].fillna(0) + df_detalhes['frete'].fillna(0)
            
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
            
            # Exibir comparativo
            st.markdown(f'<h3 style="color:#000000;">Comparativo de Preços ({len(cols_fornecedores)} Fornecedores)</h3>', unsafe_allow_html=True)
            
            def destacar_minimo(row):
                estilos = ['' for _ in range(len(row))]
                melhor = row['Melhor Preço']
                for i, col_nome in enumerate(row.index):
                    if col_nome in cols_fornecedores and row[col_nome] == melhor and melhor > 0:
                        estilos[i] = 'background-color: #D4EDDA; color: #155724; font-weight: bold'
                return estilos
            
            st.dataframe(
                df_pivot.style.apply(destacar_minimo, axis=1).format(
                    subset=['Melhor Preço'] + cols_fornecedores, precision=2
                ),
                use_container_width=True, hide_index=True
            )
            
            # Estratégias
            with st.container(border=True):
                st.markdown('<h3 style="color: #000000;">Estratégia de Fechamento</h3>', unsafe_allow_html=True)
                col1, col2, col3 = st.columns(3)
                
                if col1.button("▼ Menor Custo", type="primary" if st.session_state['estrategia_ativa'] == 'menor_preco' else "secondary", use_container_width=True):
                    st.session_state['estrategia_ativa'] = 'menor_preco'
                    st.rerun()
                if col2.button("👤 Fornecedor Único", type="primary" if st.session_state['estrategia_ativa'] == 'fornecedor_unico' else "secondary", use_container_width=True):
                    st.session_state['estrategia_ativa'] = 'fornecedor_unico'
                    st.rerun()
                if col3.button("⏱️ Menor Prazo", type="primary" if st.session_state['estrategia_ativa'] == 'menor_prazo' else "secondary", use_container_width=True):
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
                        
                        df_prazo = df_detalhes.pivot_table(
                            index=index_cols,
                            columns='fornecedor_nome',
                            values='prazo_entrega',
                            aggfunc='min'
                        ).reset_index()
                        
                        # Verificar quais fornecedores estão no pivot de prazo
                        cols_prazo_disponiveis = [c for c in df_prazo.columns if c in cols_fornecedores]
                        
                        if cols_prazo_disponiveis:
                            df_prazo['Vencedor_Prazo'] = df_prazo[cols_prazo_disponiveis].idxmin(axis=1)
                            
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
                        st.error("Coluna 'Prazo' não encontrada.")
                
                # Exibir pedidos
                if pedidos:
                    st.write("---")
                    st.markdown('<h3 style="color: #0047AB;">Pedidos Gerados</h3>', unsafe_allow_html=True)
                    for fornecedor, df_pedido in pedidos.items():
                        if isinstance(df_pedido, list):
                            df_pedido = pd.DataFrame(df_pedido)
                        if not df_pedido.empty:
                            total = (df_pedido['quantidade'] * df_pedido['Valor Unitário']).sum()
                            with st.expander(f"{fornecedor} — Total: R$ {total:,.2f}", expanded=True):
                                col_busca, _ = st.columns([3.5, 6.5])
                                with col_busca:
                                    busca = st.text_input(f"Buscar produto", key=f"busca_{fornecedor}")
                                
                                df_exibir = df_pedido.copy()
                                if busca:
                                    df_exibir = df_pedido[
                                        df_pedido['codigo_produto'].astype(str).str.contains(busca, case=False, na=False) |
                                        df_pedido['nome_produto'].astype(str).str.contains(busca, case=False, na=False)
                                    ]
                                
                                st.dataframe(df_exibir, use_container_width=True, hide_index=True)
                                
                                # Botões PDF e Enviar
                                col_pdf, col_enviar = st.columns(2)
                                with col_pdf:
                                    pdf_bytes = self.gerar_pdf_pedido(fornecedor, df_pedido)
                                    st.download_button(
                                        "📄 Baixar PDF",
                                        pdf_bytes,
                                        f"Pedido_{fornecedor}.pdf",
                                        "application/pdf",
                                        key=f"pdf_{fornecedor}",
                                        use_container_width=True
                                    )
                                with col_enviar:
                                    if st.button("✅ Enviar Pedido", key=f"enviar_{fornecedor}", type="primary", use_container_width=True):
                                        # Salvar pedido confirmado no banco
                                        if 'pedidos_confirmados' not in st.session_state:
                                            st.session_state['pedidos_confirmados'] = {}
                                        st.session_state['pedidos_confirmados'][fornecedor] = df_pedido.to_dict('records')
                                        st.success(f"Pedido enviado para {fornecedor}!")
                                        st.rerun()


@st.dialog("Manual do Usuário", width="large")
def exibir_manual():
    perfil = st.session_state.get('perfil_usuario', 'CLIENTE')
    
    if perfil in ("ADM", "CLIENTE"):
        st.markdown("""
## 📘 Manual do Sistema - Área de Compras

Este manual detalha o funcionamento de cada etapa do processo de suprimentos.

---

### 1️⃣ Gerar Cobertura (Análise de Necessidade)
Identifica **o quê**, **quanto** e **onde** comprar.

**Funcionalidades:**
* **Análise de Giro:** Calcula a velocidade de venda de cada item por filial
* **Sugestão Automática:** Baseada no estoque atual e lead time de entrega
* **Filtros Inteligentes:** Por filial, marca, grupo, subgrupo e produto
* **Dois Modos de Análise:**
  - **Sugestão de Compra:** Identifica itens com estoque baixo
  - **Análise de Sobra:** Identifica itens com excesso de estoque

**Como usar:**
1. Selecione os filtros desejados (filial, marca, grupo, etc.)
2. Escolha o tipo de análise
3. Configure os parâmetros (dias de cobertura)
4. Clique em "GERAR ANÁLISE"
5. Revise os resultados e adicione ao orçamento

---

### 2️⃣ Gerar Orçamento (Preparação de Pedidos)
Organiza e distribui os itens para cotação com fornecedores.

**Funcionalidades:**
* **Atribuição Automática:** Sistema sugere fornecedor baseado na marca do produto
* **Atribuição Manual:** Você pode escolher fornecedor específico
* **Edição de Quantidades:** Ajuste as quantidades antes de enviar
* **Filtros e Busca:** Localize produtos rapidamente

**Como usar:**
1. Os itens vêm da tela "Gerar Cobertura"
2. Revise os fornecedores sugeridos (coluna editável)
3. Ajuste as quantidades se necessário
4. Clique em "REGISTRAR PEDIDO AO FORNECEDOR"
5. O sistema cria pedidos separados por fornecedor automaticamente

---

### 3️⃣ Inteligência de Compra (Análise Comparativa)
Compara cotações e identifica a melhor opção de compra.

**Funcionalidades:**
* **Upload de Cotações:** Envie planilhas dos fornecedores (.xlsx)
* **Comparativo Automático:** Sistema destaca o melhor preço
* **Três Estratégias de Fechamento:**
  - **Menor Preço:** Escolhe o fornecedor mais barato por item
  - **Fornecedor Único:** Concentra compra em um fornecedor
  - **Menor Entrega:** Prioriza o prazo de entrega mais rápido
* **Geração de PDF:** Crie pedidos formatados para envio

**Como usar:**
1. Faça upload das planilhas de cotação dos fornecedores
2. O sistema identifica automaticamente cada fornecedor pelo nome do arquivo
3. Escolha a estratégia de fechamento
4. Revise os pedidos gerados
5. Baixe os PDFs para envio aos fornecedores

---

### 💡 Dicas Importantes
* Mantenha os nomes dos arquivos de cotação com o nome do fornecedor
* Use os filtros para análises mais precisas
* Revise sempre as quantidades antes de registrar pedidos
* O sistema salva automaticamente suas edições

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

    elif menu in ("Gerar Cobertura", "Gerar Orçamento", "Inteligência de Compra"):
        if perfil not in ("ADM", "CLIENTE"):
            st.warning("Você não tem permissão para acessar esta área.")
            st.stop()
        if menu == "Gerar Cobertura":
            app.tela_cobertura()
        elif menu == "Gerar Orçamento":
            app.tela_orcamento()
        elif menu == "Inteligência de Compra":
            app.tela_analise_retorno()

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
        if st.button("📖 Manual do Usuário", key="btn_manual_v6_final", use_container_width=True):
            st.session_state.show_manual = True

    if st.session_state.get('show_manual', False):
        exibir_manual()
        st.session_state.show_manual = False