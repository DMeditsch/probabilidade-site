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
    Este aplicativo utiliza um banco de dados SQLite que é extraído automaticamente
    de um arquivo compactado (database.gz) quando o aplicativo é iniciado.
    
    O banco de dados descompactado será automaticamente excluído quando você 
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
                st.info("Descompactando o banco de dados... Por favor, aguarde...")
                status = st.status("Descompactando...", expanded=True)
                with gzip.open(GZIP_PATH, 'rb') as f_in:
                    with open(DB_PATH, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                status.update(label="Banco de dados pronto!", state="complete", expanded=False)
            return True
        else:
            st.error(f"Arquivo compactado não encontrado: {GZIP_PATH}")
            st.error("O aplicativo não pode funcionar sem o arquivo de banco de dados.")
            st.markdown("""
            ### Solução:
            1. Certifique-se de que o arquivo `database.gz` está presente no mesmo diretório do aplicativo.
            2. Se o arquivo foi renomeado, renomeie-o de volta para `database.gz`.
            3. Se o arquivo está em outro diretório, mova-o para o diretório do aplicativo.
            4. Reinicie o aplicativo após resolver o problema.
            """)
            return False
    except Exception as e:
        st.error(f"Erro ao descompactar o banco de dados: {e}")
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

# Registrar a função para ser executada ao encerrar o aplicativo
atexit.register(excluir_database)

# Descompactar o banco de dados
if not descompactar_database():
    st.stop()

# Verificar se o banco de dados existe após descompactar
if not os.path.exists(DB_PATH):
    st.error(f"Banco de dados não encontrado: {DB_PATH}")
    st.info("Certifique-se que o arquivo database.gz está presente no diretório.")
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
         "Busca por ID", "Visualização Personalizada", "Consulta SQL"]
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
    - Origem: Extraído de {GZIP_PATH}
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

# Executar o aplicativo
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"Ocorreu um erro durante a execução do aplicativo: {e}")
    finally:
        # O banco de dados será excluído pela função registrada no atexit
        pass 