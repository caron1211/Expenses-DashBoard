from datetime import date
import warnings
# Dash configuration
import dash_core_components as dcc
import dash_html_components as html
import dash_table
from dash.dependencies import Input, Output
from flask_login import current_user
import plotly.express as px

import database.credit_mgt as cm
import database.transaction_mgt as tm
import pandas as pd

from server import app
from pandas import DataFrame
months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']

warnings.filterwarnings("ignore")

today = date.today()

# fig = px.pie(df, values='SUM(amount)', names='sector', title='Population of European continent')

# Create success layout
layout = html.Div(children=[
    dcc.Location(id='url_login_success', refresh=True),
    html.Div(
        className="container",
        children=[
            html.Div(
                html.Div(
                    className="row",
                    children=[
                        html.Div(
                            className="ten columns",
                            children=[
                                html.Br(),
                            ]
                        ),
                        html.Div(
                            className="two columns",
                            # children=html.A(html.Button('LogOut'), href='/')
                            children=[
                                html.Br(),
                                html.Button(id='upload-button', children='Upload Data', n_clicks=0)
                            ]
                        )
                    ]
                )
            )
        ]
    ),

    html.H1("טבלת הוצאות", style={'text-align': 'center'}),

    dcc.Dropdown(
        id='credit-card-dropdown',
        value='all'
    ),

    html.Br(),
    dcc.DatePickerSingle(

        id='my-date-picker-single',
        date=today,
        display_format='MMM , YYYY',
    ),

    html.Br(),
    html.Br(),

    html.Div(id='table-container'),
    html.Br(),

    html.H1("חלוקה לפי ענפים", style={'text-align': 'center'}),

    html.Div(id='pie-chart'),
    html.Br(),

    html.H1("הוצאה שנתית לפי ענפים", style={'text-align': 'center'}),
    dcc.Dropdown(id='sector-drop'),
    dcc.DatePickerSingle(

        id='year-picker',
        date=today,
        display_format='YYYY',
    ),

    html.Div(id='bar-chart')

])


# Create callbacks
@app.callback(Output('url_login_success', 'pathname'),
              [Input('upload-button', 'n_clicks')])
def logout_dashboard(n_clicks):
    if n_clicks > 0:
        return '/upload'


@app.callback(
    Output('bar-chart', 'children'),
    [Input('sector-drop', 'value'),
     Input('year-picker', 'date')])
def update_output(sector_value, year):
    if sector_value is not None:
            p = tm.get_transaction_by_year(year)
            p['date'] = pd.to_datetime(p['date'])
            c = p.query('sector ==  @sector_value').groupby(p['date'].dt.strftime('%B'))['amount'].sum().reset_index()
            c['date'] = pd.Categorical(c['date'], categories=months, ordered=True)
            c.sort_values(by='date', ascending=True, inplace=True)
            fig = px.bar(c, x='date', y='amount')
            return dcc.Graph(
                    figure=fig
                )


@app.callback(
    [Output('credit-card-dropdown', 'options'),
     Output('sector-drop', 'options')],
    [Input('url_login_success', 'pathname')])
def update_output(pathname):
    if current_user.is_authenticated:
        cards_res = cm.get_user_cards(current_user.id)
        cards_list = [
            {'label': i['card_num'], 'value': i['card_num']} for i in cards_res
        ]
    cards_list.append({'label': 'All cards', 'value': 'all'})

    sector_res = tm.get_all_sectors()
    sector_list = [
        {'label': i, 'value': i} for i in sector_res['sector']
    ]
    return [cards_list, sector_list]


@app.callback(
    [Output('table-container', 'children'),
     Output('pie-chart', 'children')],
    [Input('credit-card-dropdown', 'value'),
     Input('my-date-picker-single', 'date'), ])
def update_graph(card_value, date_value):
    # return 'You have selected  {} and {} '.format(card_value,date_value)
    if card_value is not None and date_value is not None:
        date_object = date.fromisoformat(date_value)
        month = date_object.month
        year = date_object.year
        if card_value == 'all':
            tr_df = tm.get_transaction_by_date(month,year)

        else:
            tr_df = tm.get_transaction_by_card_and_date(card_value,month,year)

        df_groupby = tr_df.groupby(['sector'],  as_index=False).sum(['amount'])
        fig = px.pie(df_groupby, values='amount', names='sector')

        return [
            dash_table.DataTable(
                id='datatable',
                columns=[
                    {"name": i, "id": i, "deletable": True}
                    for i in tr_df.columns
                ],
                data=tr_df.to_dict('records'),
                filter_action="native",
                sort_action="native",
                sort_mode="single",

            ),
            dcc.Graph(
                figure=fig
            )
        ]