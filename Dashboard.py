# Script permettant la réalisation d'un dashboard et l'affichage des données actualisées

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from sqlalchemy import create_engine
import pandas as pd
import plotly.graph_objects as go


#Carte 1 


# Informations de connexion via Pgadmin 
DB_USER = 'the_user'
DB_PASSWORD = 'the_password'
DB_HOST = 'jedha-lime-dw.cd4ke4ysdo.eu-west-3.rds.amazonaws.com'
DB_PORT = '5432'
DB_NAME = 'velib'

# Créer la connexion à la BD
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Fonction pour colorier par pourcentage de disponibilité
def color_by_availability(availability_percent):
    if availability_percent < 11:
        return 'red'  
    elif availability_percent < 50:
        return 'orange'  
    else:
        return 'darkblue' 

def fetch_data_from_db():
    
    # Créez l'objet moteur de SQLAlchemy 
    engine = create_engine(DATABASE_URL)
    
    # Requête SQL
    sql = """
    SELECT
        s.stationcode,
        s.name,
        s.capacity,
        s.longitude,
        s.latitude,
        f.ebike,
        f.mechanical,
        f.request_timestamp
    FROM
        public.stations s
    JOIN (
        SELECT
            stationcode,
            ebike,
            mechanical,
            request_timestamp
        FROM
            public.fleet
        WHERE (stationcode, request_timestamp) IN (
            SELECT
                stationcode,
                MAX(request_timestamp) as latest_timestamp
            FROM
                public.fleet
            GROUP BY
                stationcode
        )
    ) f ON s.stationcode = f.stationcode;
    """

    
    # Exécutez la requête SQL et retournez un DataFrame
    df = pd.read_sql_query(sql, engine)
    return df

# Récupérer les données de la bd
df = fetch_data_from_db()

# Calculer le nombre total de vélos (mécaniques et électriques) à chaque station
df['total_bikes'] = df['mechanical'] + df['ebike']

# Calculer le pourcentage de disponibilité des vélos à chaque station
df['percent_total_bikes'] = df.apply(
    lambda row: round(float((row["total_bikes"] / row["capacity"] * 100)) if row["capacity"] != 0 else 0), 
    axis=1
)

# Trois groupes selon la disponibilité (pour la légende)
low_avail = df[df['percent_total_bikes'] < 11]
med_avail = df[(df['percent_total_bikes'] >= 11) & (df['percent_total_bikes'] < 50)]
high_avail = df[df['percent_total_bikes'] >= 50]

# Appliquer la fonction pour créer la colonne de couleur
df['availability_color'] = df['percent_total_bikes'].apply(color_by_availability)

# Créer et configurer la première figure
fig1 = go.Figure()


# Ajoutez une trace pour la faible disponibilité
fig1.add_trace(go.Scattermapbox(
    lat=low_avail['latitude'],
    lon=low_avail['longitude'],
    mode='markers',
    marker=dict(
        size=10,
        color='red',
    ),
    name='<11%',
    hoverinfo='text',
    text=low_avail.apply(lambda row: f"Nom: {row['name']}<br>Code de la station: {row['stationcode']}<br>Vélos électriques: {row['ebike']}<br>Total des vélos: {row['total_bikes']}<br>Pourcentage de disponibilité: {row['percent_total_bikes']}%", axis=1)
))

# Répétez pour la disponibilité moyenne et élevée
fig1.add_trace(go.Scattermapbox(
    lat=med_avail['latitude'],
    lon=med_avail['longitude'],
    mode='markers',
    marker=dict(
        size=10,
        color='darkorange',
    ),
    name='10-49%',
    hoverinfo='text',
    text=med_avail.apply(lambda row: f"Nom: {row['name']}<br>Code de la station: {row['stationcode']}<br>Vélos électriques: {row['ebike']}<br>Total des vélos: {row['total_bikes']}<br>Pourcentage de disponibilité: {row['percent_total_bikes']}%", axis=1)
))

fig1.add_trace(go.Scattermapbox(
    lat=high_avail['latitude'],
    lon=high_avail['longitude'],
    mode='markers',
    marker=dict(
        size=10,
        color='darkblue',
    ),
    name='>=50%',
    hoverinfo='text',
    text=high_avail.apply(lambda row: f"Nom: {row['name']}<br>Code de la station: {row['stationcode']}<br>Vélos électriques: {row['ebike']}<br>Total des vélos: {row['total_bikes']}<br>Pourcentage de disponibilité: {row['percent_total_bikes']}%", axis=1)
))

# Récupération du timestamp le plus récent pour l'affichage
latest_timestamp = df['request_timestamp'].max().strftime('%Y-%m-%d %H:%M:%S')

# Configuration de la carte
fig1.update_layout(
    mapbox_style="carto-positron",
    mapbox_zoom=10,
    mapbox_center={"lat": df['latitude'].mean(), "lon": df['longitude'].mean()},
    title="Disponibilité des Vélibs dans Paris et environs",
    title_x=0.5,
    annotations=[{
        'text': f"Dernière mise à jour: {latest_timestamp}",
        'align': 'right',
        'showarrow': False,
        'xref': 'paper',
        'yref': 'paper',
        'x': 1,
        'y': 1,
        'xanchor': 'right',
        'yanchor': 'top',
        'font': {'size': 12}
    }]
)

# Supprimer l'affichage automatique des couleurs dans la légende
colors = ['red', 'darkorange', 'darblue']
fig1.update_traces(showlegend=False)

# Légende  pour la disponibilité
legend_info = {
    'red': '<11%',
    'darkorange': '10> <50%',
    'darkblue': '>= 50%'
}

for color, text in legend_info.items():
    fig1.add_trace(go.Scattermapbox(
        mode='markers',
        lon=[None],
        lat=[None],
        marker=dict(color=color, size=12),
        name=text,
        legendgroup='Disponibilité en %'
    ))
    
    nombre_unique = df['stationcode'].nunique()
print("Nombre de valeurs uniques dans 'stationcode':", nombre_unique)

# Afficher la carte
#fig1.show()

# Pause entre les deux cartes (facultatif)
import time
time.sleep(2)

#carte 2 

# Informations de connexion via Pgadmin 
DB_USER = 'the_user'
DB_PASSWORD = 'the_password'
DB_HOST = 'jedha-lime-dw.cd4ke4ysedo.eu-west-3.rds.amazonaws.com'
DB_PORT = '5432'
DB_NAME = 'velib'

# Créer la connexion à la BD
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

#Couleur par nombre de ebikes
def color_by_ebikes(ebike):
    if ebike <= 1:
        return 'red'  
    elif 2 <= ebike <= 4:
        return 'darkorange'  
    else:
        return 'darkblue'  

#Taille par disponibilité
def size_by_availability(availability_percent):
    if availability_percent > 50:
        return 5
    elif 11 < availability_percent <= 50:
        return 10
    else:
        return 20

def fetch_data_from_db():
    # Créez l'objet moteur de SQLAlchemy 
    engine = create_engine(DATABASE_URL)
    
    # Requête SQL
    sql = """
    SELECT
        s.stationcode,
        s.name,
        s.capacity,
        s.longitude,
        s.latitude,
        f.ebike,
        f.mechanical,
        f.request_timestamp
    FROM
        public.stations s
    JOIN (
        SELECT
            stationcode,
            ebike,
            mechanical,
            request_timestamp
        FROM
            public.fleet
        WHERE (stationcode, request_timestamp) IN (
            SELECT
                stationcode,
                MAX(request_timestamp) as latest_timestamp
            FROM
                public.fleet
            GROUP BY
                stationcode
        )
    ) f ON s.stationcode = f.stationcode;
    """
    # Exécutez la requête SQL et retournez un DataFrame
    df = pd.read_sql_query(sql, engine)
    return df

# Récupérer les données de la bd
df = fetch_data_from_db()

# Calculer le nombre total de vélos (mécaniques et électriques) à chaque station
df['total_bikes'] = df['mechanical'] + df['ebike']

df['percent_total_bikes'] = df.apply(lambda row: round(float((row["total_bikes"] / row["capacity"] * 100)) if row["capacity"] != 0 else 0), axis=1)

# Appliquer les fonctions pour créer les colonnes de couleur et de taille
df['ebike_color'] = df['ebike'].apply(color_by_ebikes)
df['circle_size'] = df['percent_total_bikes'].apply(size_by_availability)

# Création de la carte 2 avec Plotly
fig2 = go.Figure()

# Ajouter les marqueurs de type cercle pour les stations
fig2.add_trace(go.Scattermapbox(
    lat=df['latitude'],
    lon=df['longitude'],
    mode='markers',
    marker=dict(
        size=df['circle_size'],
        color=df['ebike_color'],
        symbol='circle'
    ),
    hoverinfo='text',
    text=df.apply(lambda row: f"Nom: {row['name']}<br>Code de la station: {row['stationcode']}<br>Vélos électriques: {row['ebike']}<br>Total des vélos: {row['total_bikes']}<br>Pourcentage de disponibilité: {row['percent_total_bikes']}%", axis=1)
))


# Récupération du timestamp le plus récent pour l'affichage
latest_timestamp = df['request_timestamp'].max().strftime('%Y-%m-%d %H:%M:%S')

# Type de map background et personnalisation
fig2.update_layout(
    mapbox_style="carto-positron",
    mapbox_zoom=10,
    mapbox_center={"lat": df['latitude'].mean(), "lon": df['longitude'].mean()},
    title="Disponibilité des Vélibs dans Paris et environs",
    title_x=0.5,  # Centre le titre
    mapbox=dict(
        bearing=0,
        pitch=0,
        zoom=10,
    ),
    annotations=[{
        'text': f"Dernière mise à jour: {latest_timestamp}",  
        'align': 'right',
        'showarrow': False,
        'xref': 'paper',
        'yref': 'paper',
        'x': 1,  
        'y': 1, 
        'xanchor': 'right',
        'yanchor': 'top',
        'font': {'size': 12}
    }]
)

# Supprimer l'affichage automatique des couleurs dans la légende
colors = ['darkorange', 'darkblue', 'lightblue']
fig2.update_traces(showlegend=False)

# Légende disponibilité en %
legend_values = [10, 50, 100] 
legend_labels = ['< 11%', '11-50%', '> 50%'] 

for value, label in zip(legend_values, legend_labels):
    fig2.add_trace(go.Scattermapbox(
        mode='markers',
        lon=[None],
        lat=[None],
        marker=dict(size=size_by_availability(value), color='grey'),
        name=label,
        legendgroup='availability'
    ))

# Légende E-bikes
ebike_legend_info = {
    'red': '0-1 ebike',
    'darkorange': '2-4 ebikes',
    'darkblue': '> 5 ebikes'
}

for color, text in ebike_legend_info.items():
    fig2.add_trace(go.Scattermapbox(
        mode='markers',
        lon=[None],
        lat=[None],
        marker=dict(color=color, size=12),
        name=text,
        legendgroup='ebike'
    ))


# Afficher la carte 2
#fig2.show()

# Initialisation de l'application Dash
app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("Visualisation des Vélibs à Paris"),
    dcc.RadioItems(
        id='map-selection',
        options=[
            {'label': 'Disponibilité des Vélibs en %', 'value': 'map1'},  
            {'label': 'Disponibilité + eBikes', 'value': 'map2'}  
        ],
        value='map1'  # La valeur par défaut détermine quel graphique est affiché en premier
    ),
    dcc.Graph(id='map-display', style={'height': '80vh', 'width': '100%'})
])

@app.callback(
    Output('map-display', 'figure'),
    [Input('map-selection', 'value')]
)
def update_map(map_choice):
    if map_choice == 'map1':
        
        fig = fig1
    elif map_choice == 'map2':
        
        fig = fig2
    else:
        fig = {}
    
    if fig:
        fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0}, height=600)  # Ajustement de la hauteur
    return fig

# Exécution de l'application
if __name__ == '__main__':
    app.run_server(debug=False)
