import requests
import json
import pandas as pd
import datetime
import streamlit as st
from streamlit_javascript import st_javascript

# authentication token on Piwik PRO API

token_url='https://auchan.piwik.pro/auth/token'

creds={
    "grant_type": "client_credentials",
    "client_id": st.secrets.client_id,
    "client_secret": st.secrets.client_secret,

}

token_data=requests.post(token_url, data=creds,headers={'Accept': 'application/json'},json={"key":"value"}).json()

token=token_data['access_token']
token_headers={"Authorization": "Bearer " + token,  "Accept-Encoding": "gzip"}

print("Token authentication OK")


# data query from the api ---------------------------------------------------

today=datetime.datetime.now()
today_query=today.strftime("%Y-%m-%d")


data_final=[]

query_sessions={
    "date_from": today_query,
    "date_to": today_query,
    "website_id": "40271453-d501-414f-8c05-ccdff13df350",
        "columns": [
        {
            "column_id": "session_total_ecommerce_conversions"
        },

        ],
    "filters": {
    "operator": "and",
    "conditions": []
    },
    "offset": 0,
    "limit": 100000,
    "format": "json"
}

session_query_url='https://auchan.piwik.pro/api/analytics/v1/sessions/'

piwik_response=requests.post(session_query_url, headers=token_headers, data=json.dumps(query_sessions))

response_code=piwik_response.status_code

print(f'session response code is: {response_code}')

piwik_sessions=piwik_response.json()


if response_code == 200 :
    df_sessions=pd.DataFrame(piwik_sessions['data'])
else:
    print(piwik_sessions)


df_sessions.columns=["sessions","users","timestamp","ecommerce_conversions"]

df_sessions["timestamp"]=pd.to_datetime(df_sessions["timestamp"])

# ECOMMERCE CONVERSIONS DATAFRAME ----------------------

query_orders={
    "date_from": today_query,
    "date_to": today_query,
    "website_id": "40271453-d501-414f-8c05-ccdff13df350",
        "columns": [
        		{
			"transformation_id": "to_start_of_hour",
			"column_id": "timestamp"
		},    
		{
			"column_id": "order_id"
		},
		{
			"column_id": "revenue"
		},
        ],
    "filters": {
        "operator": "and",
                "conditions": [
                    {
                        "operator": "or",
                        "conditions": [
                            {
                                "column_id": "event_type",
                                "condition": {
                                    "operator": "eq",
                                    "value": 9
                                }
                            }
                        ]
                    }
                ]
	},    
    "offset": 0,
    "limit": 100000,
    "format": "json"
}

order_query_url='https://auchan.piwik.pro/api/analytics/v1/query'

piwik_order_response=requests.post(order_query_url, headers=token_headers, data=json.dumps(query_orders))

response_code_orders=piwik_order_response.status_code

print(f'order response code is: {response_code_orders}')

piwik_orders=piwik_order_response.json()


if response_code_orders == 200 :
    df_orders=pd.DataFrame(piwik_orders['data'])
else:
    print(piwik_orders)

df_orders.columns=["timestamp","order_id","revenue"]
df_orders["timestamp"]=pd.to_datetime(df_orders["timestamp"])



# ---------------- LAST YEAR Performance DATAFRAME
file='https://docs.google.com/spreadsheets/d/e/2PACX-1vRkv-ljvMJXbmeRaJR20QjtHDTPlcENBCuGpkgzq6sqtEhPtd5iQ5SbOSzIlstKiZRhqXbo9zD6qUe7/pub?gid=0&single=true&output=csv'

df_last_year=pd.read_csv(file,parse_dates=[0])

print("last year data loaded")

# Get today's total orders, sales, sessions

today_sales=round(df_orders["revenue"].sum())

today_orders=round(df_orders["order_id"].nunique())

today_sessions=df_sessions["sessions"].nunique()

today_aov=round(today_sales/today_orders,1)

today_conversion=round(today_orders/today_sessions*100,1)

# get last years data to get the delta for the metrics
last_year_day = today - datetime.timedelta(days=364)

current_hour=int(today.strftime("%H"))-1

last_year_sales_now=float(df_last_year["sales"][df_last_year["date"]==last_year_day.strftime("%Y-%m-%d")])*current_hour/24
delta_sales=round((today_sales-last_year_sales_now)/last_year_sales_now*0.90*100,2)

last_year_orders_now=int(df_last_year["orders"][df_last_year["date"]==last_year_day.strftime("%Y-%m-%d")])*current_hour/24
delta_orders=round((today_orders-last_year_orders_now)/last_year_orders_now*0.95*100,2)

last_year_sessions_now=int(df_last_year["sessions"][df_last_year["date"]==last_year_day.strftime("%Y-%m-%d")])*current_hour/24
delta_sessions=round((today_sessions-last_year_sessions_now)/last_year_sessions_now*100,2)

last_year_aov_now=float(df_last_year["aov"][df_last_year["date"]==last_year_day.strftime("%Y-%m-%d")])
delta_aov=round((today_aov-last_year_aov_now)/last_year_aov_now*0.95*100,2)

last_year_conversion_now=float(df_last_year["conversion_rate"][df_last_year["date"]==last_year_day.strftime("%Y-%m-%d")])
delta_conversion=round((today_conversion-last_year_conversion_now)/last_year_conversion_now*0.95*100,2)

# get session data from last 60 minutes by source

now = datetime.datetime.now() - datetime.timedelta(minutes=1)
mins_ago = now - datetime.timedelta(minutes=60)

df_live_session_orders=df_sessions.loc[df_sessions["timestamp"].between(mins_ago, now)]


# dataframe to get hourly sessions from today


df_today_total_sessions=df_sessions[['timestamp','sessions']]
df_today_total_sessions['timestamp']=df_sessions["timestamp"].dt.strftime('%H:00')
df_today_hour_sessions=df_today_total_sessions.groupby('timestamp').nunique().reset_index()
df_today_hour_sessions.columns=['Hora','Visitas']

# ---------------- Last 30 days average session per hour DATAFRAME

yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
last_30 = datetime.datetime.now() - datetime.timedelta(days=31)

yesterday_query=yesterday.strftime("%Y-%m-%d")
last_30_query=last_30.strftime("%Y-%m-%d")

query_hours={
    "date_from": last_30_query,
    "date_to": yesterday_query,
    "website_id": "40271453-d501-414f-8c05-ccdff13df350",
        "columns": [
		{
			"column_id": "local_hour"
		},
		{
			"column_id": "sessions"
		}
        ],
    	"order_by": [
		[
			0,
			"asc"
		]
	],    
    "limit": 30,
    "format": "json"
}

order_query_url='https://auchan.piwik.pro/api/analytics/v1/query'

piwik_hour_response=requests.post(order_query_url, headers=token_headers, data=json.dumps(query_hours))

response_code_hour=piwik_hour_response.status_code

print(f'hour response code is: {response_code_hour}')

piwik_hourly_sessions=piwik_hour_response.json()


if response_code_hour == 200 :
    df_hours=pd.DataFrame(piwik_hourly_sessions['data'])
else:
    print(piwik_hourly_sessions)

df_hours.columns=["Hora","Visitas"]

df_hours["Visitas"]=round(df_hours["Visitas"]/30)
df_hours["Hora"]= pd.to_datetime(df_hours['Hora'], format='%H')
df_hours["Hora"]=df_hours["Hora"].dt.strftime('%H:%M')

df_today=df_hours.merge(df_today_hour_sessions,on="Hora",how="left",suffixes=[" Média 30 dias"," Hoje"])


# Total timeframe sessions metric

df_total_sessions=df_live_session_orders["sessions"].count()

# Total timeframe Session Order Conversions

df_total_orders=df_live_session_orders["ecommerce_conversions"].sum()


df_total_conversion=round(df_total_orders/df_total_sessions*100,1)


# streamlit data visualization

# SETTING PAGE CONFIG TO WIDE MODE AND ADDING A TITLE 
st.set_page_config(layout="wide", page_title="🥦 Dados Fresquinhos auchan.pt 🍅", page_icon='https://auchaneeu.auchan.pt/content/themes/jumboeeu/assets/images/favicon.png')


st.title(f"🥦 Dados Fresquinhos auchan.pt 🍅")

today_pt=now.strftime("%d-%m-%Y")
st.header(f'{today_pt}')

# markdown to change the font size for the metrics
st.markdown(
    """
<style>

@import url('https://fonts.googleapis.com/css2?family=Roboto');

    html, body, [class*="css"]  {
    font-family: 'Roboto', sans-serif;
    }

.appview-container {
    position: relative;
}
.appview-container::before{
  content:"";
  text-align: center;
  position: absolute;
  display: flex;
  background-image: url(https://www.auchan.fr/xch/v5/content-renderer/sav_2022.11.16-9/images/auchan-logo-desktop.svg);
    width: 200px;
    height: 48px;
    background-repeat: no-repeat;
    background-size: 100%;
    margin-top: 20px;
    margin-left: 20px;
    z-index: 999999;
}

h1 {
    text-align: center;
    display: flex;
    align-items: center;
    justify-content: center;
    width: auto;
}

h2 {
    text-align: center;
    display: flex;
    align-items: center;
    justify-content: center;
    width: auto;
    font-size: 32px;
}

[data-testid="metric-container"] {
  text-align: center;
  margin: auto;
}

[data-testid="stMarkdownContainer"] p {
    font-size: 44px;
}

[data-testid="stMetricValue"] {
    font-size: 60px;
}

[data-testid="stMetricDelta"] {
    font-size: 40px;
}
</style>
""",
    unsafe_allow_html=True,
)


col1, col2, col3,col4 = st.columns([2,2,2,2])

# col1.subheader(f"KPIs de Hoje")
col1.metric(label="🤑 Vendas", value=f'{today_sales} €', delta=f'{delta_sales} %')
col1.metric(label="🧾 Ticket Médio", value=f'{today_aov} €', delta=f'{delta_aov} %')

col2.metric(label="📦 Encomendas", value=today_orders,delta=f'{delta_orders} %')
col2.metric(label="⏱📦 Enc. 60 mins ", value=df_total_orders)

col3.metric(label="🚶 Visitas", value=today_sessions, delta=f'{delta_sessions} %')
col3.metric(label="⏱🚶 Visitas 60 mins ", value=df_total_sessions)

col4.metric(label="🎯 Tx Conversão", value=f'{today_conversion} %', delta=f'{delta_conversion} %')
col4.metric(label="⏱🎯 Tx Conversão 60mins", value=f'{df_total_conversion} %')

#area/line chart for last 60 minute sessions
st.text(" ")
st.text(" ")


st.subheader('🚶 Visitas por Hora (Hoje vs Média últimos 30 dias)')

st.line_chart(df_today,x='Hora',use_container_width=True)

# last 60 minute session data

now_time=now.strftime("%Y-%m-%d %H:%M:%S")

st.caption(f"Última atualização: {now_time}")
st.caption(f"Versão 1.0")


# https://pypi.org/project/streamlit-javascript/
# https://stackoverflow.com/a/32913581/16129184
# automated script to refresh page 600000 ms = 10 mins

st_javascript("""window.setTimeout( function() {
  window.location.reload();
}, 600000);""")

print(f"page update: {now_time}")
