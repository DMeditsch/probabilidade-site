import streamlit as st
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import os
import gzip
import shutil
import atexit
import json
from google import genai
from google.genai import types

# Configurar o layout da página para wide mode
st.set_page_config(
    page_title="MetObjects Explorer",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Título do aplicativo
st.title("🏛️ Metropolitan Museum of Art - Explorador de Dados")
st.markdown("### Análise interativa da coleção do Metropolitan Museum of Art")

# Adicionar explicação sobre o processo de banco de dados
with st.expander("ℹ️ Informações sobre o banco de dados", expanded=False):
    st.markdown("""
    Este aplicativo utiliza um banco de dados SQLite para armazenar e consultar 
    a coleção do Metropolitan Museum of Art.
    
    O banco de dados será automaticamente excluído quando você 
    fechar o aplicativo para economizar espaço em disco.
    
    Se você encontrar problemas com o banco de dados, tente reiniciar o aplicativo.
    """)

# Caminho para o banco de dados
DB_PATH = "metobjects.db"
GZIP_PATH = "database.gz"

# Função para descompactar o arquivo database.gz
def descompactar_database():
    try:
        # Verificar se o arquivo compactado existe
        if os.path.exists(GZIP_PATH):
            # Verificar se o banco já está descompactado
            if not os.path.exists(DB_PATH):
                st.info("Preparando o banco de dados... Por favor, aguarde...")
                status = st.status("Preparando...", expanded=True)
                with gzip.open(GZIP_PATH, 'rb') as f_in:
                    with open(DB_PATH, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                status.update(label="Banco de dados pronto!", state="complete", expanded=False)
            return True
        else:
            st.error(f"Arquivo de banco de dados não encontrado")
            st.error("O aplicativo não pode funcionar sem o arquivo de banco de dados.")
            st.markdown("""
            ### Solução:
            1. Verifique se o arquivo necessário está presente no diretório do aplicativo.
            2. Reinicie o aplicativo após resolver o problema.
            """)
            return False
    except Exception as e:
        st.error(f"Erro ao preparar o banco de dados: {e}")
        return False

# Função para excluir o banco de dados quando o aplicativo for encerrado
def excluir_database():
    if os.path.exists(DB_PATH):
        try:
            # Tentar fechar todas as conexões com o banco de dados
            try:
                conn = sqlite3.connect(DB_PATH)
                conn.close()
            except:
                pass
            
            # Excluir o arquivo do banco de dados
            os.remove(DB_PATH)
            print(f"Banco de dados {DB_PATH} excluído com sucesso!")
        except Exception as e:
            print(f"Erro ao excluir o banco de dados: {e}")

# Função para consultar a API do Gemini para gerar consultas SQL ou analisar dados
def consultar_ia(pergunta, schema_info):
    try:
        # Inicializar o cliente Gemini com a chave da API fixa
        client = genai.Client(api_key=API_KEY)
        
        # Preparar o prompt com informações sobre o esquema do banco
        prompt = f"""
        Você é um assistente especializado em SQL para o banco de dados do Metropolitan Museum of Art.
        
        Informações sobre o esquema do banco de dados:
        {schema_info}
        
        A consulta do usuário é: "{pergunta}"
        
        Se o usuário estiver pedindo uma consulta SQL:
        1. Gere APENAS o código SQL que atenda à solicitação
        2. Use a sintaxe SQLite
        3. Coloque aspas duplas em nomes de colunas com espaços
        4. Retorne apenas o código SQL sem explicações
        5. As categorias devem ser exibidas pelo nome completo entre aspas duplas exemplo: "Object Name"
        
        Se o usuário estiver fazendo uma pergunta geral sobre o banco de dados:
        1. Forneça uma resposta clara e direta
        2. Mencione também uma consulta SQL que pode ser usada para obter esses dados
        
        Resposta:
        """
        
        # Configurar o modelo e a solicitação
        model = "gemini-2.0-flash-lite"
        contents = [
            types.Content(
                role="user",
                parts=[types.Part.from_text(text=prompt)],
            ),
        ]
        
        generate_content_config = types.GenerateContentConfig(
            temperature=0.2,
            top_p=0.95,
            top_k=40,
            max_output_tokens=2048,
            response_mime_type="text/plain",
        )
        
        # Fazer a chamada da API
        response = client.models.generate_content(
            model=model,
            contents=contents,
            config=generate_content_config,
        )
        
        return response.text
    except Exception as e:
        return f"Erro ao consultar a IA: {e}"

# Registrar a função para ser executada ao encerrar o aplicativo
atexit.register(excluir_database)

# Descompactar o banco de dados
if not descompactar_database():
    st.stop()

# Verificar se o banco de dados existe após descompactar
if not os.path.exists(DB_PATH):
    st.error(f"Banco de dados não encontrado: {DB_PATH}")
    st.info("Verifique se os arquivos necessários estão presentes no diretório.")
    st.stop()

# Função para executar consultas SQL
@st.cache_data(ttl=3600)
def executar_consulta(query):
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Erro ao executar consulta: {e}")
        return pd.DataFrame()

# Função para obter as colunas da tabela
@st.cache_data(ttl=3600)
def obter_colunas():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(metobjects)")
    colunas = [col[1] for col in cursor.fetchall()]
    conn.close()
    return colunas

# Função para obter valores únicos de uma coluna
@st.cache_data(ttl=3600)
def obter_valores_unicos(coluna):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(f'SELECT DISTINCT "{coluna}" FROM metobjects WHERE "{coluna}" != "" ORDER BY "{coluna}"')
    valores = [val[0] for val in cursor.fetchall()]
    conn.close()
    return valores

# Função para obter o esquema do banco de dados
@st.cache_data(ttl=7200)
def obter_schema_info():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Obter informações sobre a tabela
    cursor.execute("PRAGMA table_info(metobjects)")
    colunas = cursor.fetchall()
    
    # Obter alguns exemplos de dados para cada coluna
    schema_info = "Tabela: metobjects\n\nColunas:\n"
    
    for col in colunas:
        col_id, nome, tipo, notnull, default_val, pk = col
        schema_info += f"- {nome} ({tipo})\n"
        
        # Obter alguns valores distintos para esta coluna (se não for muito grande)
        try:
            cursor.execute(f'SELECT DISTINCT "{nome}" FROM metobjects WHERE "{nome}" IS NOT NULL AND "{nome}" != "" LIMIT 5')
            exemplos = cursor.fetchall()
            if exemplos:
                schema_info += f"  Exemplos: {', '.join([str(ex[0]) for ex in exemplos])}\n"
        except:
            pass
    
    # Obter contagens para algumas colunas importantes
    cursor.execute("SELECT COUNT(*) FROM metobjects")
    total_rows = cursor.fetchone()[0]
    schema_info += f"\nTotal de registros: {total_rows}\n"
    
    # Obter estatísticas para algumas colunas categóricas importantes
    for coluna in ["Department", "Culture", "Object Name", "Classification"]:
        try:
            cursor.execute(f'SELECT COUNT(DISTINCT "{coluna}") FROM metobjects WHERE "{coluna}" != ""')
            distinct_count = cursor.fetchone()[0]
            schema_info += f"Total de {coluna} distintos: {distinct_count}\n"
        except:
            pass
    
    conn.close()
    return schema_info

# Função para obter estatísticas básicas
@st.cache_data(ttl=3600)
def obter_estatisticas():
    stats = {}
    
    # Total de objetos
    query = "SELECT COUNT(*) FROM metobjects"
    stats['total_objetos'] = executar_consulta(query).iloc[0, 0]
    
    # Total de departamentos
    query = "SELECT COUNT(DISTINCT Department) FROM metobjects WHERE Department != ''"
    stats['total_departamentos'] = executar_consulta(query).iloc[0, 0]
    
    # Total de culturas
    query = "SELECT COUNT(DISTINCT Culture) FROM metobjects WHERE Culture != ''"
    stats['total_culturas'] = executar_consulta(query).iloc[0, 0]
    
    # Total de artistas
    query = 'SELECT COUNT(DISTINCT "Artist Display Name") FROM metobjects WHERE "Artist Display Name" != ""'
    stats['total_artistas'] = executar_consulta(query).iloc[0, 0]
    
    # Total de tipos de objetos
    query = 'SELECT COUNT(DISTINCT "Object Name") FROM metobjects WHERE "Object Name" != ""'
    stats['total_tipos_objetos'] = executar_consulta(query).iloc[0, 0]
    
    return stats

# Função para criar visualização de departamentos
def visualizar_departamentos():
    query = """
    SELECT Department, COUNT(*) as Count 
    FROM metobjects 
    WHERE Department != '' 
    GROUP BY Department 
    ORDER BY Count DESC
    """
    
    df = executar_consulta(query)
    
    # Calcular a porcentagem
    total = df['Count'].sum()
    df['Porcentagem'] = (df['Count'] / total * 100).round(2)
    
    # Criar gráfico interativo com Plotly
    fig = px.bar(
        df, 
        y='Department', 
        x='Count', 
        orientation='h',
        text=df['Porcentagem'].apply(lambda x: f'{x:.2f}%'),
        color='Count',
        color_continuous_scale='Blues',
        title='Distribuição de Objetos por Departamento'
    )
    
    fig.update_layout(
        xaxis_title='Número de Objetos',
        yaxis_title='Departamento',
        height=600
    )
    
    return fig, df

# Função para criar visualização de objetos por tipo
def visualizar_objetos_por_tipo():
    query = """
    SELECT "Object Name", COUNT(*) as Count 
    FROM metobjects 
    WHERE "Object Name" != '' 
    GROUP BY "Object Name" 
    ORDER BY Count DESC
    """
    
    df = executar_consulta(query)
    
    # Criar gráfico interativo com Plotly
    fig = px.bar(
        df.head(50), 
        y='Object Name', 
        x='Count', 
        orientation='h',
        color='Count',
        color_continuous_scale='Viridis',
        title='Top 50 Tipos de Objetos (mostrando os 50 mais comuns de um total de ' + str(len(df)) + ')'
    )
    
    fig.update_layout(
        xaxis_title='Número de Objetos',
        yaxis_title='Tipo de Objeto',
        height=700
    )
    
    return fig, df

# Função para criar visualização de culturas
def visualizar_culturas():
    query = """
    SELECT Culture, COUNT(*) as Count 
    FROM metobjects 
    WHERE Culture != '' 
    GROUP BY Culture 
    ORDER BY Count DESC
    """
    
    df = executar_consulta(query)
    
    # Criar gráfico interativo com Plotly
    fig = px.pie(
        df.head(30), 
        values='Count', 
        names='Culture',
        title='Distribuição de Objetos por Cultura (mostrando as 30 principais de um total de ' + str(len(df)) + ')'
    )
    
    fig.update_layout(height=600)
    
    return fig, df

# Função para filtrar objetos 
def filtrar_objetos():
    # Obter as colunas disponíveis
    colunas = obter_colunas()
    
    # Criar opções para filtros nas colunas mais comuns
    st.subheader("Filtros")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        departamento = st.selectbox(
            "Departamento", 
            ["Todos"] + obter_valores_unicos("Department"),
            index=0
        )
    
    with col2:
        cultura = st.selectbox(
            "Cultura", 
            ["Todas"] + obter_valores_unicos("Culture"),
            index=0
        )
    
    with col3:
        tipo_objeto = st.text_input("Tipo de Objeto (contém):", "")
    
    col4, col5, col6 = st.columns(3)
    
    with col4:
        artista = st.text_input("Artista (contém):", "")
    
    with col5:
        data_objeto = st.text_input("Data (contém):", "")
    
    with col6:
        is_domain_publico = st.selectbox(
            "Domínio Público", 
            ["Qualquer", "Sim", "Não"],
            index=0
        )
    
    # Construir a consulta SQL
    query = 'SELECT * FROM metobjects WHERE 1=1'
    
    if departamento != "Todos":
        query += f' AND Department = "{departamento}"'
    
    if cultura != "Todas":
        query += f' AND Culture = "{cultura}"'
    
    if tipo_objeto:
        query += f' AND "Object Name" LIKE "%{tipo_objeto}%"'
    
    if artista:
        query += f' AND "Artist Display Name" LIKE "%{artista}%"'
    
    if data_objeto:
        query += f' AND "Object Date" LIKE "%{data_objeto}%"'
    
    if is_domain_publico != "Qualquer":
        value = "True" if is_domain_publico == "Sim" else "False"
        query += f' AND "Is Public Domain" = "{value}"'
    
    # Executar a consulta
    df = executar_consulta(query)
    
    return df

# Função para visualizar dados de um objeto específico
def visualizar_objeto(objeto_id):
    query = f'SELECT * FROM metobjects WHERE "Object ID" = "{objeto_id}"'
    df = executar_consulta(query)
    
    if len(df) == 0:
        st.error("Objeto não encontrado")
        return
    
    # Obter os dados do objeto
    obj = df.iloc[0]
    
    col1, col2 = st.columns([2, 3])
    
    with col1:
        st.subheader(obj["Title"] if obj["Title"] else "Sem título")
        
        if obj["Artist Display Name"]:
            st.write(f"**Artista:** {obj['Artist Display Name']}")
        
        if obj["Object Date"]:
            st.write(f"**Data:** {obj['Object Date']}")
        
        if obj["Culture"]:
            st.write(f"**Cultura:** {obj['Culture']}")
        
        if obj["Medium"]:
            st.write(f"**Meio:** {obj['Medium']}")
        
        if obj["Dimensions"]:
            st.write(f"**Dimensões:** {obj['Dimensions']}")
        
        if obj["Credit Line"]:
            st.write(f"**Crédito:** {obj['Credit Line']}")
        
        if obj["Department"]:
            st.write(f"**Departamento:** {obj['Department']}")
        
        # Link para o objeto no site do museu
        if obj["Object ID"]:
            object_url = f"https://www.metmuseum.org/art/collection/search/{obj['Object ID']}"
            st.markdown(f"[Ver no site do Metropolitan Museum 🔗]({object_url})")
    
    with col2:
        # Link Resource pode conter a URL da imagem
        if obj["Link Resource"] and obj["Is Public Domain"] == "True":
            st.image(obj["Link Resource"], caption=obj["Title"], use_column_width=True)
        else:
            st.info("Imagem não disponível ou não está em domínio público")
            
            # Se tiver URL do Wikidata, mostrar link
            if obj["Object Wikidata URL"]:
                st.markdown(f"[Ver no Wikidata 🔗]({obj['Object Wikidata URL']})")

# Função para criar visualização personalizada
def criar_visualizacao_personalizada():
    st.subheader("Criar Visualização Personalizada")
    
    # Obter as colunas disponíveis
    colunas = obter_colunas()
    colunas_categoricas = [col for col in colunas if col not in ["Object ID", "Dimensions"]]
    
    # Seleção do tipo de gráfico
    tipo_grafico = st.selectbox(
        "Tipo de Gráfico",
        ["Barras", "Pizza", "Dispersão", "Linha"],
        index=0
    )
    
    # Configuração do gráfico
    col1, col2 = st.columns(2)
    
    with col1:
        coluna_x = st.selectbox("Selecione a coluna para agrupar", colunas_categoricas)
        
        limite = st.slider("Limite de dados", 5, 50, 15)
        
    with col2:
        # Agregação para contagem
        if tipo_grafico in ["Barras", "Pizza"]:
            agregacao = "COUNT(*)"
            legenda_y = "Contagem"
        else:
            # Para gráficos de dispersão/linha, precisa de uma segunda variável
            colunas_numericas = ["Object ID"]  # Poderia incluir outras se tivéssemos colunas numéricas
            coluna_y = st.selectbox("Selecione a coluna para o eixo Y", colunas_numericas)
            agregacao = f'AVG("{coluna_y}")'
            legenda_y = f"Média de {coluna_y}"
    
    # Construir consulta base
    query = f"""
    SELECT "{coluna_x}", {agregacao} as Y
    FROM metobjects
    WHERE "{coluna_x}" != ""
    GROUP BY "{coluna_x}"
    ORDER BY Y DESC
    LIMIT {limite}
    """
    
    # Executar a consulta
    df = executar_consulta(query)
    
    # Criar visualização
    if len(df) > 0:
        if tipo_grafico == "Barras":
            fig = px.bar(
                df, 
                x=coluna_x, 
                y="Y",
                color=coluna_x,
                title=f'Distribuição por {coluna_x}',
                labels={coluna_x: coluna_x, "Y": legenda_y}
            )
        
        elif tipo_grafico == "Pizza":
            fig = px.pie(
                df, 
                values="Y", 
                names=coluna_x,
                title=f'Distribuição por {coluna_x}',
                hole=0.3
            )
        
        elif tipo_grafico == "Dispersão":
            fig = px.scatter(
                df, 
                x=coluna_x, 
                y="Y",
                color=coluna_x,
                title=f'{legenda_y} por {coluna_x}',
                labels={coluna_x: coluna_x, "Y": legenda_y},
                size="Y",
                size_max=60
            )
        
        elif tipo_grafico == "Linha":
            # Ordenar por nome da coluna para linha
            df = df.sort_values(by=coluna_x)
            
            fig = px.line(
                df, 
                x=coluna_x, 
                y="Y",
                markers=True,
                title=f'{legenda_y} por {coluna_x}',
                labels={coluna_x: coluna_x, "Y": legenda_y}
            )
        
        fig.update_layout(height=600)
        st.plotly_chart(fig, use_container_width=True)
        
        # Exibir os dados
        with st.expander("Ver dados"):
            st.dataframe(df)
    else:
        st.warning("Não há dados suficientes para criar o gráfico selecionado")

# Função para executar SQL personalizado
def executar_sql_personalizado():
    st.subheader("Consulta SQL Personalizada")
    
    # Exemplos de consultas
    st.markdown("""
    **Exemplos de consultas:**
    ```sql
    -- Contagem de objetos por departamento
    SELECT Department, COUNT(*) as Count 
    FROM metobjects 
    WHERE Department != '' 
    GROUP BY Department 
    ORDER BY Count DESC;
    
    -- Artistas com mais obras
    SELECT "Artist Display Name", COUNT(*) as Count 
    FROM metobjects 
    WHERE "Artist Display Name" != '' 
    GROUP BY "Artist Display Name" 
    ORDER BY Count DESC;
    
    -- Pinturas do século XIX
    SELECT "Object Number", Title, "Artist Display Name", "Object Date"
    FROM metobjects 
    WHERE "Object Name" LIKE '%painting%' 
    AND "Object Date" LIKE '%19th Century%';
    ```
    """)
    
    # Editor de consulta
    query = st.text_area(
        "Digite sua consulta SQL:",
        height=200,
        help="Escreva uma consulta SQL para executar no banco de dados"
    )
    
    if st.button("Executar Consulta"):
        if query:
            # Executar a consulta
            df = executar_consulta(query)
            
            # Exibir os resultados
            if len(df) > 0:
                st.dataframe(df)
                
                # Opção para baixar como CSV
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Baixar como CSV",
                    data=csv,
                    file_name="resultado_consulta.csv",
                    mime="text/csv"
                )
            else:
                st.info("A consulta não retornou resultados")
        else:
            st.error("Por favor, digite uma consulta SQL")

# Função para a interface de consulta com IA
def consulta_com_ia():
    st.subheader("🤖 Consulta com Inteligência Artificial")
    
    # Exemplos de consultas
    st.markdown("""
    ### 💡 Exemplos do que você pode perguntar:
    
    - Mostre todas as pinturas de Claude Monet
    - Quais são os 10 artistas com mais obras no museu?
    - Quantas esculturas egípcias existem na coleção?
    - Conte o número de objetos por século
    - Quais são os objetos mais antigos da coleção?
    """)
    
    # Entrada da pergunta
    pergunta = st.text_area(
        "Digite sua pergunta em linguagem natural:",
        height=100,
        placeholder="Ex: Mostre todas as pinturas de Vincent van Gogh ordenadas por data"
    )
    
    # Inicializar variáveis na sessão se não existirem
    if "consulta_sql_gerada" not in st.session_state:
        st.session_state.consulta_sql_gerada = ""
    if "mostrar_resultados" not in st.session_state:
        st.session_state.mostrar_resultados = False
    
    # Dividir a tela em duas colunas apenas para a entrada e informações
    col1, col2 = st.columns([1, 2])
    
    # Primeira coluna para consulta e botões
    with col1:
        # Botão para consultar a IA
        if st.button("Consultar IA", type="primary"):
            if pergunta:
                with st.spinner("A IA está processando sua consulta..."):
                    # Obter informações do esquema
                    schema_info = obter_schema_info()
                    
                    # Consultar a IA
                    resposta = consultar_ia(pergunta, schema_info)
                    
                    # Verificar se a resposta parece ser SQL
                    is_sql_query = "SELECT" in resposta.upper() and "FROM" in resposta.upper()
                    
                    if is_sql_query:
                        # Extrair apenas a consulta SQL se houver texto adicional
                        # Encontrar a consulta SQL entre os sinais SELECT e ;
                        import re
                        sql_match = re.search(r'(SELECT.+?);', resposta, re.DOTALL | re.IGNORECASE)
                        
                        if sql_match:
                            consulta_sql = sql_match.group(1) + ";"
                        else:
                            consulta_sql = resposta.strip()
                        
                        # Armazenar a consulta gerada na sessão
                        st.session_state.consulta_sql_gerada = consulta_sql
                        st.session_state.resposta_texto = ""
                    else:
                        # Armazenar resposta textual
                        st.session_state.resposta_texto = resposta
                        st.session_state.consulta_sql_gerada = ""
                    
                    # Resetar flag de resultados
                    st.session_state.mostrar_resultados = False
            else:
                st.error("Por favor, digite uma pergunta.")
    
        # Se temos uma consulta SQL gerada, mostrar e permitir executá-la
        if st.session_state.consulta_sql_gerada:
            st.subheader("Consulta SQL gerada:")
            st.code(st.session_state.consulta_sql_gerada, language="sql")
            
            # Botão para executar a consulta
            if st.button("Executar Consulta SQL"):
                st.session_state.mostrar_resultados = True
                st.rerun()
        
        # Se temos uma resposta textual, mostrar
        if "resposta_texto" in st.session_state and st.session_state.resposta_texto:
            st.subheader("Resposta:")
            st.write(st.session_state.resposta_texto)
    
    # Segunda coluna para informações sobre o banco de dados
    with col2:
        # Mostrar dicas sobre a estrutura do banco
        st.subheader("Estrutura do Banco de Dados")
        st.markdown("""
        O banco de dados contém a tabela `metobjects` com informações sobre a coleção 
        do Metropolitan Museum of Art.
        
        **Colunas principais:**
        - `Object ID`: Identificador único do objeto
        - `Title`: Título da obra
        - `Artist Display Name`: Nome do artista
        - `Object Date`: Data da obra
        - `Department`: Departamento do museu
        - `Culture`: Cultura associada à obra
        - `Medium`: Material/meio utilizado
        - `Classification`: Classificação da obra
        - `Object Name`: Tipo do objeto (pintura, escultura, etc.)
        - `Is Public Domain`: Se a obra está em domínio público
        """)
        
        # Mostrar alguns exemplos de valores
        with st.expander("Ver exemplos de valores", expanded=False):
            # Departamentos
            st.subheader("Departamentos")
            df_dept = executar_consulta("SELECT DISTINCT Department FROM metobjects WHERE Department != '' LIMIT 20")
            st.dataframe(df_dept)
            
            # Culturas
            st.subheader("Culturas")
            df_cult = executar_consulta("SELECT DISTINCT Culture FROM metobjects WHERE Culture != '' LIMIT 20")
            st.dataframe(df_cult)
            
            # Objetos
            st.subheader("Tipos de Objetos")
            df_obj = executar_consulta("SELECT DISTINCT \"Object Name\" FROM metobjects WHERE \"Object Name\" != '' LIMIT 20")
            st.dataframe(df_obj)
    
    # Mostrar resultados em tela cheia (fora das colunas)
    if st.session_state.mostrar_resultados and st.session_state.consulta_sql_gerada:
        st.markdown("---")  # Separador horizontal
        
        try:
            # Executar a consulta
            df = executar_consulta(st.session_state.consulta_sql_gerada)
            
            # Exibir os resultados
            if len(df) > 0:
                st.subheader("Resultados:")
                st.dataframe(df, use_container_width=True)  # Usar toda a largura disponível
                
                # Opção para baixar como CSV
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Baixar como CSV",
                    data=csv,
                    file_name="resultado_ia.csv",
                    mime="text/csv"
                )
            else:
                st.info("A consulta não retornou resultados")
        except Exception as e:
            st.error(f"Erro ao executar a consulta: {e}")

# Interface principal
def main():
    # Barra lateral
    st.sidebar.image("https://upload.wikimedia.org/wikipedia/commons/7/7a/The_MET_logo.png", width=200)
    st.sidebar.title("Navegação")
    
    # Menu principal
    pagina = st.sidebar.radio(
        "Escolha uma seção:",
        ["Visão Geral", "Filtrar Objetos", "Análise por Departamento", 
         "Análise por Tipo de Objeto", "Análise por Cultura", 
         "Busca por ID", "Visualização Personalizada", "Consulta SQL", "Consulta com IA"]
    )
    
    # Obter estatísticas gerais
    stats = obter_estatisticas()
    
    # Mostrar informações do banco de dados na barra lateral
    st.sidebar.subheader("Informações do Banco")
    
    # Exibir informações sobre o arquivo do banco
    tamanho_db = os.path.getsize(DB_PATH) / (1024 * 1024)  # Tamanho em MB
    st.sidebar.info(f"""
    💾 **Banco de Dados:**
    - Arquivo: {DB_PATH}
    - Tamanho: {tamanho_db:.2f} MB
    - Status: Temporário (será excluído ao fechar)
    """)
    
    st.sidebar.info(f"""
    📊 **Estatísticas:**
    - Total de objetos: {stats['total_objetos']:,}
    - Departamentos: {stats['total_departamentos']}
    - Culturas: {stats['total_culturas']}
    - Artistas: {stats['total_artistas']:,}
    - Tipos de objetos: {stats['total_tipos_objetos']:,}
    """)
    
    # Footer na barra lateral
    st.sidebar.markdown("---")
    st.sidebar.markdown(
        "Desenvolvido para análise da coleção do [Metropolitan Museum of Art](https://www.metmuseum.org/)"
    )
    
    # Conteúdo principal
    if pagina == "Visão Geral":
        st.subheader("📊 Visão Geral da Coleção")
        
        # Métricas em cards
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total de Objetos", f"{stats['total_objetos']:,}")
        col2.metric("Departamentos", stats['total_departamentos'])
        col3.metric("Culturas", stats['total_culturas'])
        col4.metric("Artistas", f"{stats['total_artistas']:,}")
        
        st.markdown("---")
        
        # Mostrar algumas visualizações principais
        st.subheader("Distribuição por Departamento")
        fig_dept, df_dept = visualizar_departamentos()
        st.plotly_chart(fig_dept, use_container_width=True)
        
        st.markdown("---")
        
        # Distribuição por tipo de objeto
        st.subheader("Top 20 Tipos de Objetos")
        fig_tipo, df_tipo = visualizar_objetos_por_tipo()
        st.plotly_chart(fig_tipo, use_container_width=True)
        
        st.markdown("---")
        
        # Distribuição por cultura
        st.subheader("Principais Culturas")
        fig_cult, df_cult = visualizar_culturas()
        st.plotly_chart(fig_cult, use_container_width=True)
    
    elif pagina == "Filtrar Objetos":
        st.subheader("🔍 Filtrar Objetos")
        
        # Função para filtrar
        df_filtrado = filtrar_objetos()
        
        # Mostrar resultados
        if len(df_filtrado) > 0:
            st.subheader(f"Resultados: {len(df_filtrado)} objetos encontrados")
            
            # Selecionar colunas para exibir
            colunas_padrao = [
                "Object Number", "Object Name", "Title", "Artist Display Name", 
                "Object Date", "Culture", "Department"
            ]
            
            colunas_disponiveis = df_filtrado.columns.tolist()
            colunas_selecionadas = st.multiselect(
                "Selecione as colunas para exibir:", 
                colunas_disponiveis,
                default=colunas_padrao
            )
            
            if colunas_selecionadas:
                st.dataframe(df_filtrado[colunas_selecionadas])
                
                # Opção para baixar resultados
                csv = df_filtrado[colunas_selecionadas].to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Baixar como CSV",
                    data=csv,
                    file_name="objetos_filtrados.csv",
                    mime="text/csv"
                )
            else:
                st.warning("Selecione pelo menos uma coluna para exibir")
        else:
            st.info("Nenhum objeto encontrado com os filtros selecionados")
    
    elif pagina == "Análise por Departamento":
        st.subheader("🏛️ Análise por Departamento")
        
        # Visualização de departamentos
        fig_dept, df_dept = visualizar_departamentos()
        st.plotly_chart(fig_dept, use_container_width=True)
        
        # Dados em tabela
        st.subheader("Dados por Departamento")
        st.dataframe(df_dept)
        
        # Análise adicional: Objetos mais comuns por departamento
        st.subheader("Objetos mais comuns por Departamento")
        
        # Selecionar departamento
        departamento = st.selectbox(
            "Selecione um departamento:", 
            obter_valores_unicos("Department")
        )
        
        # Consulta para tipos de objetos no departamento
        query = f"""
        SELECT "Object Name", COUNT(*) as Count 
        FROM metobjects 
        WHERE Department = '{departamento}'
        GROUP BY "Object Name" 
        ORDER BY Count DESC
        LIMIT 10
        """
        
        df_objetos = executar_consulta(query)
        
        # Mostrar gráfico
        fig = px.bar(
            df_objetos, 
            x='Object Name', 
            y='Count',
            color='Count',
            color_continuous_scale='Teal',
            title=f'Top 10 Tipos de Objetos no Departamento: {departamento}'
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
    elif pagina == "Análise por Tipo de Objeto":
        st.subheader("🖼️ Análise por Tipo de Objeto")
        
        # Visualização de tipos de objetos
        fig_tipo, df_tipo = visualizar_objetos_por_tipo()
        st.plotly_chart(fig_tipo, use_container_width=True)
        
        # Dados em tabela
        st.subheader("Dados por Tipo de Objeto")
        st.dataframe(df_tipo)
        
        # Análise adicional: Departamentos por tipo de objeto
        st.subheader("Departamentos por Tipo de Objeto")
        
        # Buscar os tipos de objeto mais comuns
        tipos_comuns = obter_valores_unicos("Object Name")  # Remover limitação
        
        # Selecionar tipo de objeto
        tipo_objeto = st.selectbox(
            "Selecione um tipo de objeto:", 
            tipos_comuns
        )
        
        # Consulta para departamentos com esse tipo
        query = f"""
        SELECT Department, COUNT(*) as Count 
        FROM metobjects 
        WHERE "Object Name" = '{tipo_objeto}'
        AND Department != ''
        GROUP BY Department 
        ORDER BY Count DESC
        LIMIT 10
        """
        
        df_depts = executar_consulta(query)
        
        # Mostrar gráfico
        fig = px.pie(
            df_depts, 
            values='Count', 
            names='Department',
            title=f'Distribuição de {tipo_objeto} por Departamento',
            hole=0.3
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
    elif pagina == "Análise por Cultura":
        st.subheader("🌎 Análise por Cultura")
        
        # Visualização de culturas
        fig_cult, df_cult = visualizar_culturas()
        st.plotly_chart(fig_cult, use_container_width=True)
        
        # Dados em tabela
        st.subheader("Dados por Cultura")
        st.dataframe(df_cult)
        
        # Análise adicional: Objetos mais comuns por cultura
        st.subheader("Objetos mais comuns por Cultura")
        
        # Selecionar cultura
        cultura = st.selectbox(
            "Selecione uma cultura:", 
            obter_valores_unicos("Culture")
        )
        
        # Consulta para tipos de objetos na cultura
        query = f"""
        SELECT "Object Name", COUNT(*) as Count 
        FROM metobjects 
        WHERE Culture = '{cultura}'
        GROUP BY "Object Name" 
        ORDER BY Count DESC
        LIMIT 10
        """
        
        df_objetos = executar_consulta(query)
        
        # Mostrar gráfico
        fig = px.bar(
            df_objetos, 
            x='Object Name', 
            y='Count',
            color='Count',
            color_continuous_scale='Viridis',
            title=f'Top 10 Tipos de Objetos na Cultura: {cultura}'
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
    elif pagina == "Busca por ID":
        st.subheader("🔎 Busca por ID")
        
        col1, col2 = st.columns([1, 2])
        
        with col1:
            # Input para ID
            object_id = st.text_input("Digite o ID do objeto:")
            
            if st.button("Buscar"):
                if object_id:
                    visualizar_objeto(object_id)
                else:
                    st.error("Por favor, digite um ID de objeto")
            
            # Ou selecionar um objeto aleatório
            if st.button("Objeto Aleatório"):
                query = """
                SELECT "Object ID" FROM metobjects 
                WHERE "Is Public Domain" = 'True' 
                AND "Link Resource" != '' 
                ORDER BY RANDOM() 
                LIMIT 1
                """
                result = executar_consulta(query)
                if len(result) > 0:
                    random_id = result.iloc[0, 0]
                    visualizar_objeto(random_id)
        
        with col2:
            # Lista de exemplos
            st.subheader("Exemplos de IDs")
            query = """
            SELECT "Object ID", "Object Name", "Title", "Artist Display Name"
            FROM metobjects 
            WHERE "Is Public Domain" = 'True' 
            AND "Link Resource" != '' 
            ORDER BY RANDOM() 
            LIMIT 30
            """
            df_examples = executar_consulta(query)
            st.dataframe(df_examples)
    
    elif pagina == "Visualização Personalizada":
        criar_visualizacao_personalizada()
    
    elif pagina == "Consulta SQL":
        executar_sql_personalizado()
        
    elif pagina == "Consulta com IA":
        consulta_com_ia()

# Executar o aplicativo
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"Ocorreu um erro durante a execução do aplicativo: {e}")
    finally:
        # O banco de dados será excluído pela função registrada no atexit
        pass 