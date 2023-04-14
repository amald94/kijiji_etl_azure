import dash
from dash import dcc
from dash import html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import pandas as pd


listings_df = pd.read_csv("assets/listing_df.csv")
listings_df.dropna(inplace=True)
listings_df = listings_df[listings_df['bedrooms']>0]
top_df = pd.read_csv("assets/top_cities.csv")
top_df.dropna(inplace=True)

app = dash.Dash(__name__, meta_tags=[{"name": "viewport", "content": "width=device-width"}])

app.layout = html.Div([
    html.Div([
        html.Div([
        ],
            className="one-third column",
        ),
        html.Div([
            html.Div([
                html.H3("KIJIJI Rental Listings Analysis in GTA!", style={"margin-bottom": "0px", 'color': 'white'})
            ])
        ], className="one-half column", id="title"),

        html.Div([
            html.H6('Last Updated: ' + str(max(listings_df.scrape_date.unique())),
                    style={'color': 'orange'}),

        ], className="one-third column", id='title1'),

    ], id="header", className="row flex-display", style={"margin-bottom": "25px"}),

    html.Div([
        html.Div([
            html.H6(children='Total listings for the period',
                    style={
                        'textAlign': 'center',
                        'color': 'white'}
                    ),

            html.P(f"{listings_df.shape[0]}",
                   style={
                       'textAlign': 'center',
                       'color': 'orange',
                       'fontSize': 40}
                   )], className="card_container four columns",
        ),

         html.Div([
            html.H6(children='Total apt/condo listings',
                    style={
                        'textAlign': 'center',
                        'color': 'white'}
                    ),

            html.P(f"{listings_df[listings_df.type=='apartment/condo'].shape[0]}",
                   style={
                       'textAlign': 'center',
                       'color': '#dd1e35',
                       'fontSize': 40}
                   )

            ], className="card_container four columns",
        ),

        html.Div([
            html.H6(children='Total house rental listings',
                    style={
                        'textAlign': 'center',
                        'color': 'white'}
                    ),

            html.P(f"{listings_df[listings_df.type=='house'].shape[0]}",
                   style={
                       'textAlign': 'center',
                       'color': '#dd1e35',
                       'fontSize': 40}
                   )

            ], className="card_container four columns",
        ),

        ], className="row flex-display"),
        
    html.Div([
        html.Div([

                    html.P('Select City:', className='fix_label',  style={'color': 'white'}),

                     dcc.Dropdown(id='location',
                                  multi=False,
                                  clearable=False,
                                  value='toronto',
                                  placeholder='Select City',
                                  options=[{'label': c, 'value': c}
                                           for c in (listings_df['location'].unique())], className='dcc_compon'),

                    dcc.Graph(id='total_listing', config={'displayModeBar': False}, className='dcc_compon',
                     style={'margin-top': '10px'},
                     ),

                    dcc.Graph(id='apt_listing', config={'displayModeBar': False}, className='dcc_compon',
                     style={'margin-top': '10px'},
                     ),

                    dcc.Graph(id='house_listing', config={'displayModeBar': False}, className='dcc_compon',
                     style={'margin-top': '10px'},
                     ),

                    html.Div([
                        dcc.Graph(id='min_rent', config={'displayModeBar': False}, className='dcc_compon',
                        style={'width': '50%', 'display': 'inline-block'},
                        ),

                        dcc.Graph(id='max_rent', config={'displayModeBar': False}, className='dcc_compon',
                        style={'width': '50%', 'display': 'inline-block', 'float': 'right'},
                        ),
                    ], style={'display': 'flex'}),


        ], className="create_container three columns", id="cross-filter-options"),
            html.Div([
                      dcc.Graph(id='pie_chart',
                              config={'displayModeBar': 'hover'}),
                              ], className="create_container four columns"),

                    html.Div([
                        html.P('Select bed nos:', className='fix_label',  style={'color': 'white'}),
                        dcc.Dropdown(id='bed-dropdown',
                                    clearable=False,
                                    options=[{'label': t, 'value': t} for t in listings_df['bedrooms'].unique()],
                                    value=listings_df['bedrooms'].unique()[0]
                        ),
                        dcc.Graph(id="line_chart")

                    ], className="create_container five columns"),

        ], className="row flex-display"),

], id="mainContainer",
style={"display": "flex", "flex-direction": "column"})

@app.callback(Output('line_chart', 'figure'),
              [Input('location', 'value'),
              Input('bed-dropdown', 'value')])
def update_graph(location,bedrooms):
    
    colors = {
        'apartment/condo': 'orange',
        'house': '#e55467'
    }
    listings_df3 = listings_df[(listings_df['location'] == location) & (listings_df['bedrooms'] == bedrooms)]
    filtered_df = listings_df3.groupby(['location', 'type','bedrooms','rent']).size().reset_index(name='count').sort_values(by='count',ascending=False )
    return {
        'data': [
                go.Scatter(
                    x=filtered_df[filtered_df['type'] == t]['rent'],
                    y=filtered_df[filtered_df['type'] == t]['count'],
                    mode='markers',
                    marker=dict(color=colors[t]),
                    name=t
                ) for t in filtered_df['type'].unique()
            ],

        'layout': go.Layout(
             plot_bgcolor='#1f2c56',
             paper_bgcolor='#1f2c56',
             title={
                'text': 'Rent vs. Count of Properties in ' + (location.capitalize()),
                'y': 0.93,
                'x': 0.5,
                'xanchor': 'center',
                'yanchor': 'top'},
             titlefont={
                        'color': 'white',
                        'size': 20},

             hovermode='x',
             margin = dict(r = 0),
             xaxis=dict(title='<b>Rent</b>',
                        color='white',
                        showline=True,
                        showgrid=True,
                        showticklabels=True,
                        linecolor='white',
                        linewidth=2,
                        ticks='outside',
                        tickfont=dict(
                            family='Arial',
                            size=12,
                            color='white'
                        )

                ),

             yaxis=dict(title='<b>Count</b>',
                        color='white',
                        showline=True,
                        showgrid=True,
                        showticklabels=True,
                        linecolor='white',
                        linewidth=2,
                        ticks='outside',
                        tickfont=dict(
                           family='Arial',
                           size=12,
                           color='white'
                        )

                ),

            legend={
                    'orientation': 'v',
                    'bgcolor': '#1f2c56',
                    'xanchor': 'right',
                    'yanchor': 'top',
                    'x': 0.95,
                    'y': 1
                    },
            font=dict(
                family="sans-serif",
                size=12,
                color='white'),

                 )

    }

@app.callback(
    Output('min_rent', 'figure'),
    [Input('location', 'value')])
def update_confirmed(location):
    min_rent = min(listings_df[(listings_df["rent"]>500) & (listings_df['location'] == location)]["rent"])
    return {
            'data': [go.Indicator(
                    mode='number+delta',
                    value=min_rent,
                    number={'valueformat': ',',
                            'font': {'size': 20, 'color':'orange'}
                            },
                    domain={'y': [0, 1], 'x': [0, 1]})],
            'layout': go.Layout(
                title={'text': 'Min Rent',
                       'y': 1,
                       'x': 0.5,
                       'xanchor': 'center',
                       'yanchor': 'top'},
                font=dict(color='white'),
                paper_bgcolor='#1f2c56',
                plot_bgcolor='#1f2c56',
                height=80,
                margin=dict(l=10, r=10, b=10, t=10)
                ),

            }


@app.callback(
    Output('max_rent', 'figure'),
    [Input('location', 'value')])
def update_confirmed(location):
    max_rent = max(listings_df[listings_df['location'] == location]["rent"])
    return {
            'data': [go.Indicator(
                    mode='number+delta',
                    value=max_rent,
                    number={'valueformat': ',',
                            'font': {'size': 20, 'color':'orange'}
                            },
                    domain={'y': [0, 1], 'x': [0, 1]})],
            'layout': go.Layout(
                title={'text': 'Max rent',
                       'y': 1,
                       'x': 0.5,
                       'xanchor': 'center',
                       'yanchor': 'top'},
                font=dict(color='white'),
                paper_bgcolor='#1f2c56',
                plot_bgcolor='#1f2c56',
                height=80,
                margin=dict(l=10, r=10, b=10, t=10)
                ),

            }



@app.callback(
    Output('total_listing', 'figure'),
    [Input('location', 'value')])
def update_confirmed(location):
    listings_df2 = listings_df.groupby(['location', 'type']).size().reset_index(name='count')
    total = listings_df2[listings_df2['location'] == location]['count'].sum()
    return {
            'data': [go.Indicator(
                    mode='number+delta',
                    value=total,
                    number={'valueformat': ',',
                            'font': {'size': 20, 'color':'orange'}
                            },
                    domain={'y': [0, 1], 'x': [0, 1]})],
            'layout': go.Layout(
                title={'text': 'Total Listing',
                       'y': 1,
                       'x': 0.5,
                       'xanchor': 'center',
                       'yanchor': 'top'},
                font=dict(color='white'),
                paper_bgcolor='#1f2c56',
                plot_bgcolor='#1f2c56',
                height=80,
                margin=dict(l=10, r=10, b=10, t=10)
                ),

            }

@app.callback(
    Output('apt_listing', 'figure'),
    [Input('location', 'value')])
def update_confirmed(location):
    listings_df2 = listings_df.groupby(['location', 'type']).size().reset_index(name='count')
    apt_ = listings_df2[(listings_df2['location'] == location) & (listings_df2['type'] == 'apartment/condo')]['count'].iloc[-1]
    return {
            'data': [go.Indicator(
                    mode='number+delta',
                    value=apt_,
                    number={'valueformat': ',',
                            'font': {'size': 20, 'color':'orange'}
                            },
                    domain={'y': [0, 1], 'x': [0, 1]})],
            'layout': go.Layout(
                title={'text': 'Total apt/condo Listing',
                       'y': 1,
                       'x': 0.5,
                       'xanchor': 'center',
                       'yanchor': 'top'},
                font=dict(color='white'),
                paper_bgcolor='#1f2c56',
                plot_bgcolor='#1f2c56',
                height=80,
                margin=dict(l=10, r=10, b=10, t=10)
                ),

            }

@app.callback(
    Output('house_listing', 'figure'),
    [Input('location', 'value')])
def update_confirmed(location):
    listings_df2 = listings_df.groupby(['location', 'type']).size().reset_index(name='count')
    house_ = listings_df2[(listings_df2['location'] == location) & (listings_df2['type'] == 'house')]['count'].iloc[-1]
    return {
            'data': [go.Indicator(
                    mode='number+delta',
                    value=house_,
                    number={'valueformat': ',',
                            'font': {'size': 20, 'color':'orange'}
                            },
                    domain={'y': [0, 1], 'x': [0, 1]})],
            'layout': go.Layout(
                title={'text': 'Total house Listing',
                       'y': 1,
                       'x': 0.5,
                       'xanchor': 'center',
                       'yanchor': 'top'},
                font=dict(color='white'),
                paper_bgcolor='#1f2c56',
                plot_bgcolor='#1f2c56',
                height=80,
                margin=dict(l=10, r=10, b=10, t=10)
                ),

            }

@app.callback(Output('pie_chart', 'figure'),
              [Input('location', 'value')])

def update_graph(location):
    listings_df2 = listings_df.groupby(['location', 'type']).size().reset_index(name='count')
    apt_listings = listings_df2[(listings_df2['location'] == location) & (listings_df2['type'] == 'apartment/condo')]['count'].iloc[-1]
    house_listings = listings_df2[(listings_df2['location'] == location) & (listings_df2['type'] == 'house')]['count'].iloc[-1]
    colors = ['orange', '#e55467']

    return {
        'data': [go.Pie(labels=['Apt/condo', 'House'],
                        values=[apt_listings, house_listings],
                        marker=dict(colors=colors),
                        hoverinfo='label+value+percent',
                        textinfo='label+value',
                        textfont=dict(size=13),
                        hole=.7,
                        rotation=45
                        # insidetextorientation='radial',


                        )],

        'layout': go.Layout(
            # width=800,
            # height=520,
            plot_bgcolor='#1f2c56',
            paper_bgcolor='#1f2c56',
            hovermode='closest',
            title={
                'text': 'Listings breakdown in ' + (location.capitalize()),


                'y': 0.93,
                'x': 0.5,
                'xanchor': 'center',
                'yanchor': 'top'},
            titlefont={
                       'color': 'white',
                       'size': 20},
            legend={
                'orientation': 'h',
                'bgcolor': '#1f2c56',
                'xanchor': 'center', 'x': 0.5, 'y': -0.07},
            font=dict(
                family="sans-serif",
                size=12,
                color='white')
            ),


        }


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)
