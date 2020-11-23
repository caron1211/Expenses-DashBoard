import io
import base64

import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import openpyxl as xl

from server import app, User, server
import algorthim.algo as alg
from urllib.parse import quote as urlquote
from flask import Flask, send_from_directory
from database import transaction_mgt as tm
from database import credit_mgt as cm
from flask_login import current_user
import database.credit_mgt as cm

import os

ROOT_DIR = os.path.abspath(os.curdir)
UPLOAD_DIRECTORY = os.path.join(ROOT_DIR, "uploads")

if not os.path.exists(UPLOAD_DIRECTORY):
    os.makedirs(UPLOAD_DIRECTORY)

layout = html.Div([
    dcc.Location(id='url_upload', refresh=True),
    dcc.Upload(
        id='upload-data',
        children=html.Div([
            'Drag and Drop or ',
            html.A('Select Files')
        ]),
        style={
            'width': '100%',
            'height': '60px',
            'lineHeight': '60px',
            'borderWidth': '1px',
            'borderStyle': 'dashed',
            'borderRadius': '5px',
            'textAlign': 'center',
            'margin': '10px'
        },
        # Allow multiple files to be uploaded
        multiple=True
    ),
    html.H2("File List"),
    html.Div("click on the file name to remove it"),
    html.Ul(id="file-list"),
    html.Button(id='back-button', children='Go Back', n_clicks=0),
    html.Button(id='delete-button', children='Delete All', n_clicks=0)

])


def save_file(name, content):
    """Decode and store a file uploaded with Plotly Dash."""
    data = content.encode("utf8").split(b";base64,")[1]
    with open(os.path.join(UPLOAD_DIRECTORY, name), "wb") as fp:
        fp.write(base64.decodebytes(data))

    load_xl_file(name,content)


def load_xl_file(name, content):
    content_type, content_string = content.split(',')
    decoded = base64.b64decode(content_string)
    if 'xlsx' in name:
        alg.upload_xl_from_zero(io.BytesIO(decoded), current_user.id)
    else:
        print("faild")


def uploaded_files():
    """List the files in the upload directory."""
    files = []
    for filename in os.listdir(UPLOAD_DIRECTORY):
        path = os.path.join(UPLOAD_DIRECTORY, filename)
        if os.path.isfile(path):
            files.append(filename)
    return files


def file_download_link(filename):
    """Create a Plotly Dash 'A' element that downloads a file from the app."""
    location = "/download/{}".format(urlquote(filename))
    return html.A(filename, href=location)


@app.callback(
    Output("file-list", "children"),
    [Input("upload-data", "filename"), Input("upload-data", "contents")],
)
def update_output(uploaded_filenames, uploaded_file_contents):
    """Save uploaded files and regenerate the file list."""

    if uploaded_filenames is not None and uploaded_file_contents is not None:
        for name, data in zip(uploaded_filenames, uploaded_file_contents):
            save_file(name, data)

    files = uploaded_files()
    if len(files) == 0:
        return [html.Li("No files yet!")]
    else:
        return [html.Div(html.Li(file_download_link(filename))) for filename in files]


@app.callback(Output('url_upload', 'pathname'),
              [Input('back-button', 'n_clicks'), Input('delete-button', 'n_clicks')])
def logout_dashboard(n_clicks_back,n_clicks_delete):
    if n_clicks_back > 0:
        return '/success'
    if n_clicks_delete > 0:
        for filename in os.listdir(UPLOAD_DIRECTORY):
            os.remove(os.path.join(UPLOAD_DIRECTORY, filename))
        tm.del_all_transaction()
        cm.del_all_cards()
        return '/success'


@server.route("/download/<path:path>")
def download(path):
    """Serve a file from the upload directory."""
    if os.path.exists(os.path.join(UPLOAD_DIRECTORY,path)):
        os.remove(os.path.join(UPLOAD_DIRECTORY,path))
        return 'Hello, World!'
