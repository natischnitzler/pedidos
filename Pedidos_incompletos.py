#!/usr/bin/env python
# coding: utf-8

# In[20]:


import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.reflection import Inspector
from datetime import datetime, timedelta

# Establecer conexión a la base de datos
engine = sqlalchemy.create_engine('mysql+mysqlconnector://patagon_2_r:bNja0n7iF55s9cJ4@sql419.your-server.de:3306/patagon_db2')


# In[22]:


with engine.connect() as connection:
    fac = pd.read_sql_query("SELECT * FROM fven11", connection)
    fact2 = pd.read_sql_query("SELECT * FROM fven22", connection)
    nota = pd.read_sql_query("SELECT * FROM nota11", connection)
    nota2 = pd.read_sql_query("SELECT * FROM nota22", connection)
    cliente = pd.read_sql_query("SELECT * FROM cliente", connection)


# In[23]:


fact = fac[["nota","fven"]]


# In[24]:


df_fac = fact2.merge(fac, how='left', on='fven')


# In[25]:


df_fac= df_fac[["raz",'fven','cod','cant', 'fee', 'fvv','nota_x',"ven"]]


# In[26]:


df_fac = df_fac.rename(columns={
    'nota_x': "nota",
    "cant":"cant_fc"
})

cols_a_formatear = ['cant_fc', 'nota',"fven"]  # reemplaza con tus columnas

for col in cols_a_formatear:
    df_fac[col] = pd.to_numeric(df_fac[col], errors='coerce').fillna(0).astype(int)

# Convertir una columna a formato fecha
df_fac['fee'] = pd.to_datetime(df_fac['fee'], errors='coerce').dt.date


# In[27]:


df_fac = df_fac.drop_duplicates()


# In[28]:


# Paso 1: identificamos las columnas excepto 'cantidad'
columnas_sin_cantidad = [col for col in df_fac.columns if col != "cant_fc"]

# Paso 2: agrupamos por esas columnas y sumamos 'cantidad'
df_fac = df_fac.groupby(columnas_sin_cantidad, as_index=False)['cant_fc'].sum()


# In[29]:


df_nota = nota2.merge(nota, how='left', on='nota')


# In[30]:


df_nota = df_nota[["raz",'nota','cod', 'cant',"prec","val",'fee']]


# In[31]:


df_nota = df_nota.rename(columns={
    "cant":"cant_nota"
})

cols_a_formatear = ['cant_nota', 'nota']  # reemplaza con tus columnas

for col in cols_a_formatear:
    df_nota[col] = pd.to_numeric(df_nota[col], errors='coerce').fillna(0).astype(int)

# Convertir una columna a formato fecha
df_nota['fee'] = pd.to_datetime(df_nota['fee'], errors='coerce').dt.date

df_nota = df_nota.drop_duplicates()


# In[32]:


# Paso 1: identificamos las columnas excepto 'cantidad'
columnas_sin_cantidad = [col for col in df_nota.columns if col != 'cant_nota']

# Paso 2: agrupamos por esas columnas y sumamos 'cantidad'
df_nota = df_nota.groupby(columnas_sin_cantidad, as_index=False)['cant_nota'].sum()


# In[33]:


df_nota["fee"] = pd.to_datetime(df_nota["fee"], errors='coerce')
df_fac["fee"] = pd.to_datetime(df_fac["fee"], errors='coerce')


# In[34]:


df = df_nota.merge(df_fac, how='left', on=['nota', 'cod'])
df['cant_fc'] = df['cant_fc'].fillna(0)


# In[35]:


df = df[['raz_x', 'nota', 'cod', 'fee_x','fven','cant_nota', 'cant_fc',"ven"]]


# In[44]:


# Asegurarse que columnas clave estén en formato correcto
df['cant_nota'] = pd.to_numeric(df['cant_nota'], errors='coerce').fillna(0).astype(int)
df['cant_fc'] = pd.to_numeric(df['cant_fc'], errors='coerce').fillna(0).astype(int)
df['fven'] = pd.to_numeric(df['fven'], errors='coerce').fillna(0).astype(int)

from datetime import datetime, timedelta

# Hoy, ayer y fecha hace 10 días
hoy = datetime.now()
fecha_inicio = hoy - timedelta(days=1)

# Asegúrate de que la columna 'fee_x' sea del tipo datetime
df['fee_x'] = pd.to_datetime(df['fee_x'], errors='coerce')

# Ahora puedes comparar sin errores
df = df[(df['fee_x'] >= fecha_inicio) & (df['fee_x'] <= hoy)]
df = df[df["fven"]>0]


# In[60]:


df


# In[58]:


df["dif"] = round(df["cant_fc"] / df["cant_nota"], 2)
df = df[df["dif"] < 1]


# In[62]:


import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from datetime import datetime

# -------------------------
# Función para generar HTML por grupo raz_x y fven
# -------------------------
def df_por_raz_a_html(df: pd.DataFrame, titulo: str) -> str:
    html = f"<h2>{titulo}</h2>"
    for raz, df_raz in df.groupby("raz_x"):
        html += f"<h3>{raz}</h3>"
        for fven, df_fven in df_raz.groupby("fven"):
            html += f"<h4>Factura: {fven}</h4>{df_fven.to_html(index=False, border=1, justify='center', classes='tabla')}"
    return html

# -------------------------
# Estilo HTML general
# -------------------------
def generar_mensaje_html(html_tablas: str) -> str:
    return f"""
    <html>
    <head>
    <style>
        .tabla {{
            border-collapse: collapse;
            width: 100%;
            margin-bottom: 20px;
        }}
        .tabla th, .tabla td {{
            border: 1px solid #ddd;
            padding: 8px;
            text-align: center;
        }}
        .tabla th {{
            background-color: #f2f2f2;
            font-weight: bold;
        }}
        h2 {{
            color: #333;
            font-family: Arial, sans-serif;
        }}
        h3 {{
            color: #555;
            font-family: Arial, sans-serif;
            margin-top: 30px;
        }}
    </style>
    </head>
    <body>
    <p>Hola! Envío resumen de pedidos que faltaron productos por facturar :</p>
    {html_tablas}
    </body>
    </html>
    """

# -------------------------
# Diccionario de correos por vendedor
# -------------------------
correos_vendedores = {
    "OFICINA": "estrella@temponovo.cl",
    "MAX LEVI": "levy.max@gmail.com",
    "ALDO CAYAZZO": "aldo@temponovo.cl",
    "PEDRO GODOY": "pedrogodoycovarrubias7@gmail.com",
    "ALEJANDRO STARK": "starksolla@gmail.com",
    "FREDY ARCHILE": "freddynaw@gmail.com",
    "FRANCISCO CORNEJO": "tatocornejo@yahoo.es",
    "": "natalia@temponovo.cl"  # vacío, se envía solo a ti
}

# Enviar correos por vendedor
# -------------------------
hoy = datetime.now().strftime("%Y-%m-%d")
smtp_server = "srv10.akkuarios.com"
smtp_port = 587
sender_email = "natalia@temponovo.cl"
email_password = "1234"  # 

if not df.empty:
    for vendedor, df_vendedor in df.groupby("ven"):
        destinatario = correos_vendedores.get(vendedor)
        if not destinatario:
            print(f"Vendedor {vendedor} no tiene correo asignado, se omite.")
            continue

        # Crear HTML personalizado para el vendedor
        html_tablas = df_por_raz_a_html(df_vendedor, "Pedidos que no se facturaron completos")
        body = generar_mensaje_html(html_tablas)

        # Crear mensaje de correo
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = destinatario
        msg['Cc'] = "natalia@temponovo.cl, daniel@temponovo.cl"
        msg['Subject'] = f"Pedidos incompletos - {vendedor} - {hoy}"
        msg.attach(MIMEText(body, 'html'))

        try:
            server = smtplib.SMTP(smtp_server, smtp_port)
            server.starttls()
            server.login(sender_email, email_password)
            server.sendmail(
                sender_email,
                [destinatario] + ["natalia@temponovo.cl", "daniel@temponovo.cl"],
                msg.as_string()
            )
            print(f"Correo enviado a {vendedor}: {destinatario}")
        except Exception as e:
            print(f"Error al enviar correo a {vendedor}: {e}")
        finally:
            server.quit()
else:
    print("El DataFrame está vacío. No se enviaron correos.")

