from flask import Flask, render_template, request, send_file, request, jsonify
import io  # Tambahkan ini untuk mengimpor modul io
from io import BytesIO  # Add this import statement
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
import mysql.connector
from flask import request, redirect, url_for, flash  # Make sure to import flash
from flask import Flask, render_template, request, redirect, url_for, flash, session
from flask_mysqldb import MySQL
from werkzeug.security import check_password_hash, generate_password_hash
from flask import Flask, render_template, request, redirect, url_for, flash, session
from flask_mysqldb import MySQL
# from flask_bcrypt import Bcrypt
from sqlalchemy import create_engine
from functools import wraps  # Tambahkan ini
from datetime import datetime
from dateutil.relativedelta import relativedelta
from flask import session
import uuid
import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import Font
from openpyxl.worksheet.hyperlink import Hyperlink
from flask import Flask, render_template, request, send_file, make_response
from flask import Flask, render_template, request
from datetime import datetime
from dateutil.relativedelta import relativedelta

app = Flask(__name__)


# #koneksi
app.secret_key = '040104'
app.config['MYSQL_HOST'] ='localhost'
app.config['MYSQL_USER'] ='root'
app.config['MYSQL_PASSWORD'] =''
app.config['MYSQL_DB'] ='telupsin_billing2'
mysql = MySQL(app)

# Database connection details
db_config = {
    'host': 'localhost',  # Change if necessary
    'user': 'root',  # Your MySQL username
    'password':'',  # Your MySQL password
    'database':'telupsin_billing2'  # Your MySQL database name
}

#koneksi2
app.secret_key = '040104'
app.config['MYSQL_HOST'] ='localhost'
app.config['MYSQL_USER'] ='root'
app.config['MYSQL_PASSWORD'] =''
app.config['MYSQL_DB'] ='telupsin_billing2'
mysql = MySQL(app)

# Database connection details
db_config = {
    'host': 'localhost',  # Change if necessary
    'user': 'root',  # Your MySQL username
    'password':'',  # Your MySQL password
    'database':'telupsin_billing2'  # Your MySQL database name
}
# # #koneksi
# app.secret_key = '040104'
# app.config['MYSQL_HOST'] ='103.147.154.188'
# app.config['MYSQL_USER'] ='snapbill_yusup'
# app.config['MYSQL_PASSWORD'] ='Wn!Sh2Dxs9%o'
# app.config['MYSQL_DB'] ='snapbill_billing'
# mysql = MySQL(app)

# # Database connection details
# db_config = {
#     'host': '103.147.154.188',  # Change if necessary
#     'user': 'snapbill_yusup',  # Your MySQL username
#     'password':'Wn!Sh2Dxs9%o',  # Your MySQL password
#     'database':'snapbill_billing'  # Your MySQL database name
# }

# #koneksi2
# app.secret_key = '040104'
# app.config['MYSQL_HOST'] ='103.147.154.188'
# app.config['MYSQL_USER'] ='snapbill_yusup'
# app.config['MYSQL_PASSWORD'] ='Wn!Sh2Dxs9%o'
# app.config['MYSQL_DB'] ='snapbill_billing'
# mysql = MySQL(app)

# # Database connection details
# db_config = {
#     'host': '103.147.154.188',  # Change if necessary
#     'user': 'snapbill_yusup',  # Your MySQL username
#     'password':'Wn!Sh2Dxs9%o',  # Your MySQL password
#     'database':'snapbill_billing'  # Your MySQL database name
# }

# Middleware untuk proteksi login
def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get('loggedin'):
            flash('Anda harus login terlebih dahulu.', 'warning')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return wrapper

# Username & Password untuk Login
database = {'telup3mpw': '040104', 'yusup': '1'}

# Halaman Login
@app.route('/', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        name1 = request.form.get('username')
        pwd = request.form.get('password')
        
        # Validasi username dan password
        if name1 not in database or database[name1] != pwd:
            flash('Username atau Password salah!', 'danger')
            return render_template('login.html')
        else:
            # Login berhasil, simpan session
            session['loggedin'] = True
            session['username'] = name1
            flash(f'Selamat datang, {name1}!', 'success')
            return redirect(url_for('pilih_ulp'))  # Arahkan ke halaman pilih ULP

    return render_template('login.html')

# Halaman Pilih ULP
@app.route('/pilih-ulp', methods=['GET', 'POST'])
@login_required
def pilih_ulp():
    if request.method == 'POST':
        ulp = request.form.get('ulp')
        session['ulp'] = ulp

        if ulp == "ULP MEMPAWAH":
            return redirect(url_for('admin_mempawah'))
        elif ulp == "ULP SIANTAN":
            return redirect(url_for('admin_siantan'))
        elif ulp == "ULP NGABANG":
            return redirect(url_for('admin_ngabang'))
        elif ulp == "ULP TANJUNG RAYA":
            return redirect(url_for('admin_tanjungraya'))
        elif ulp == "UP3 MEMPAWAH":
            return redirect(url_for('admin_up3'))
        else:
            flash("ULP tidak dikenali.", "danger")
            return redirect(url_for('pilih_ulp'))

    return render_template('pilih_ulp.html')

# Halaman Logout
@app.route('/logout')
def logout():
    session.pop('loggedin', None)
    session.pop('username', None)
    session.pop('ulp', None)
    flash('Anda telah logout.', 'success')
    return redirect(url_for('login'))


def get_previous_blth(blth_str, months_back=1):
    date = datetime.strptime(blth_str, '%Y%m')
    prev_date = date - relativedelta(months=months_back)
    return prev_date.strftime('%Y%m')

# Create engine for MySQL connection
engine = create_engine(f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}")

def copy_dataframe(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini):
    # Create DataFrames and merge them
    juruslalulalu = pd.DataFrame({
        'BLTH': lalulalu.get('BLTH', pd.Series(dtype='object')),
        'IDPEL': lalulalu.get('IDPEL', pd.Series(dtype='int64')),
        'LWBPPAKAI': lalulalu.get('LWBPPAKAI', pd.Series(dtype='float64')) 
    })

    juruslalu = pd.DataFrame({
        'BLTH': lalu.get('BLTH', pd.Series(dtype='object')),
        'IDPEL': lalu.get('IDPEL', pd.Series(dtype='int64')),
        'LWBPPAKAI': lalu.get('LWBPPAKAI', pd.Series(dtype='float64'))
    })

    jurusakhir = pd.DataFrame({
        'BLTH': akhir.get('BLTH', pd.Series(dtype='object')),
        'IDPEL': akhir.get('IDPEL', pd.Series(dtype='int64')),
        'NAMA': akhir.get('NAMA', pd.Series(dtype='object')),
        'TARIF': akhir.get('TARIF', pd.Series(dtype='object')),
        'DAYA': akhir.get('DAYA', pd.Series(dtype='float64')),
        'SLALWBP': akhir.get('SLALWBP', pd.Series(dtype='float64')),
        'LWBPCABUT': akhir.get('LWBPCABUT', pd.Series(dtype='float64')),
        'LWBPPASANG': akhir.get('LWBPPASANG', pd.Series(dtype='float64')),
        'SAHLWBP': akhir.get('SAHLWBP', pd.Series(dtype='float64')),
        'LWBPPAKAI': akhir.get('LWBPPAKAI', pd.Series(dtype='float64')),
        'DLPD': akhir.get('DLPD', pd.Series(dtype='float64'))
    })

    # Merging DataFrames
    kroscek_temp_1 = pd.merge(juruslalulalu, juruslalu, on='IDPEL', how='right')
    kroscek_temp = pd.merge(kroscek_temp_1, jurusakhir, on='IDPEL', how='right')
    # Menghitung delta
    delta = kroscek_temp['LWBPPAKAI'] - kroscek_temp['LWBPPAKAI_y']

    # Membuat DataFrame akhir
    kroscek = pd.DataFrame({
        'BLTH': blth_kini,
        'IDPEL': kroscek_temp['IDPEL'],
        'NAMA': kroscek_temp['NAMA'],
        'TARIF': kroscek_temp['TARIF'],
        'DAYA': kroscek_temp['DAYA'].fillna(0).astype(int),
        'SLALWBP': kroscek_temp['SLALWBP'].fillna(0).astype(int),
        'LWBPCABUT': kroscek_temp['LWBPCABUT'].fillna(0).astype(int),
        'SELISIH STAN BONGKAR': (kroscek_temp['SLALWBP'].fillna(0) - kroscek_temp['LWBPCABUT'].fillna(0)).astype(int),
        'LWBP PASANG': kroscek_temp['LWBPPASANG'].fillna(0).astype(int),
        'SAHLWBP': kroscek_temp['SAHLWBP'].fillna(0).astype(int),
        'KWH SEKARANG': kroscek_temp['LWBPPAKAI'].fillna(0).astype(int),
        'KWH 1 BULAN LALU': kroscek_temp['LWBPPAKAI_y'].fillna(0).astype(int),
        'KWH 2 BULAN LALU': kroscek_temp['LWBPPAKAI_x'].fillna(0).astype(int),
        'DELTA PEMKWH': delta.fillna(0).astype(int)
    })



    # Menghitung persentase
    percentage = (delta / kroscek_temp['LWBPPAKAI_y'].replace(0, np.nan)) * 100  # Menghindari pembagian oleh 0

    # Mengatasi NaN atau inf di persentase dengan mengganti menjadi 0
    percentage = np.nan_to_num(percentage, nan=0, posinf=0, neginf=0)

    # Konversi persentase menjadi kolom di DataFrame, lalu format dengan simbol '%'
    kroscek['%'] = pd.Series(percentage).astype(int).astype(str) + '%'

    # Menyimpan kolom KET dengan kondisi yang sudah ditentukan
    kroscek['KET'] = np.where(
        kroscek_temp['LWBPPAKAI_y'].isna() | (kroscek_temp['LWBPPAKAI_y'] == 0),  # Jika NaN atau 0 di LWBPPAKAI_y
        'DIV/NA',
        np.where(
            percentage >= 40, 'NAIK',
            np.where(percentage <= -40, 'TURUN', 'AMAN')
        )
    )


    # Menambahkan kolom DLPD
    kroscek['DLPD'] = kroscek_temp['DLPD']

        # URLs for images
    path_foto1 = 'https://portalapp.iconpln.co.id/acmt/DisplayBlobServlet1?idpel='
    path_foto2 = '&blth='
    kroscek['FOTO AKHIR'] = kroscek['IDPEL'].apply(lambda x: f'<a href="{path_foto1}{x}{path_foto2}{blth_kini}" target="popup" '
              f'onclick="window.open(\'{path_foto1}{x}{path_foto2}{blth_kini}\', '
              f'\'popup\', \'width=700,height=700,scrollbars=no,toolbar=no\'); return false;">LINK FOTO</a>')
    kroscek['FOTO LALU'] = kroscek['IDPEL'].apply(lambda x: f'<a href="{path_foto1}{x}{path_foto2}{blth_lalu}" target="popup" '
              f'onclick="window.open(\'{path_foto1}{x}{path_foto2}{blth_lalu}\', '
              f'\'popup\', \'width=700,height=700,scrollbars=no,toolbar=no\'); return false;">LINK FOTO</a>')
    kroscek['FOTO LALU2'] = kroscek['IDPEL'].apply(lambda x: f'<a href="{path_foto1}{x}{path_foto2}{blth_lalulalu}" target="popup" '
              f'onclick="window.open(\'{path_foto1}{x}{path_foto2}{blth_lalulalu}\', '
              f'\'popup\', \'width=700,height=700,scrollbars=no,toolbar=no\'); return false;">LINK FOTO</a>')
    
  # Link 3 foto sekaligus, pakai 5 digit terakhir IDPEL sebagai label link
    kroscek['FOTO 3BLN'] = kroscek['IDPEL'].apply(lambda x: f'<a href="#" onclick="open3Foto(\'{x}\', \'{blth_kini}\'); return false;">{str(x)[-5:]}</a>')





    kroscek['HASIL PEMERIKSAAN'] = kroscek['KET'].apply(lambda x: f'<select class="hasil-pemeriksaan" onfocus="this.options[0].selected = true;">'
                                                      '<option value="" disabled selected hidden></option>'
                                                      '<option value="SESUAI" {"selected" if x == "SESUAI" else ""}>SESUAI</option>'
                                                      '<option value="SALAH STAN" {"selected" if x == "SALAH STAN" else ""}>SALAH STAN</option>'
                                                      '<option value="TELAT/SALAH PDL" {"selected" if x == "TELAT/SALAH PDL" else ""}>TELAT/SALAH PDL</option>'
                                                      '<option value="SALAH FOTO" {"selected" if x == "SALAH FOTO" else ""}>SALAH FOTO</option>'
                                                      '<option value="FOTO BURAM" {"selected" if x == "FOTO BURAM" else ""}>FOTO BURAM</option>'
                                                      '<option value="LEBIH TAGIH" {"selected" if x == "LEBIH TAGIH" else ""}>LEBIH TAGIH</option>'
                                                      '<option value="BUKAN FOTO KWH" {"selected" if x == "BUKAN FOTO KWH" else ""}>BUKAN FOTO KWH</option>'
                                                      '<option value="BENCANA" {"selected" if x == "BENCANA" else ""}>BENCANA</option>'
                                                      '</select>')

    kroscek['TINDAK LANJUT'] = '<textarea class="tindak-lanjut" rows="4" cols="50"></textarea>'

    kroscek['KETERANGAN'] = kroscek['KET'].apply(lambda x: '<select class="keterangan" onfocus="this.options[0].selected = true;">'
                                                       '<option value="" disabled selected hidden></option>'
                                                       '<option value="3 BULAN TIDAK DAPAT FOTO STAN">3 BULAN TIDAK DAPAT FOTO STAN</option>' 
                                                       '<option value="6 BULAN TIDAK DAPAT FOTO STAN">6 BULAN TIDAK DAPAT FOTO STAN</option>' 
                                                       '<option value="SUDAH BU">SUDAH BU</option>'
                                                       '<option value="SALAH FOTO">SALAH FOTO</option>'
                                                       '<option value="720">720</option>'
                                                       '</select>')

    
    return kroscek

def naikFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini):
    kroscek = copy_dataframe(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
    naik_df = kroscek[kroscek['%'].str.rstrip('%').astype(int) >= 40]
    return naik_df

def turunFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini):
    kroscek = copy_dataframe(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
    turun_df = kroscek[kroscek['%'].str.rstrip('%').astype(int) <= -40]
    return turun_df

def divFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini):
    kroscek = copy_dataframe(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
    # Memastikan hanya data dengan KET 'DIV/NA' yang ditampilkan di tab div
    div_df = kroscek[kroscek['KET'] == 'DIV/NA']
    return div_df



######cek informasi plg
@app.route('/cek_foto', methods=['GET', 'POST'])
def cek_foto():
    links = {}
    idpel = ''
    blth_kini = ''
    blth_lalu = ''
    blth_lalu2 = ''

    if request.method == 'POST':
        idpel = request.form.get('idpel')
        blth_kini = request.form.get('blth')
        servlet = request.form.get('servlet', '1')  # default to servlet 1

        # Hitung BLTH lalu dan 2 bulan lalu
        dt_kini = datetime.strptime(blth_kini, '%Y%m')
        blth_lalu = (dt_kini - relativedelta(months=1)).strftime('%Y%m')
        blth_lalu2 = (dt_kini - relativedelta(months=2)).strftime('%Y%m')

        servlet_path = f'https://portalapp.iconpln.co.id/acmt/DisplayBlobServlet{servlet}?idpel='
        links = {
            'FOTO_KINI': f'{servlet_path}{idpel}&blth={blth_kini}',
            'FOTO_LALU': f'{servlet_path}{idpel}&blth={blth_lalu}',
            'FOTO_LALU2': f'{servlet_path}{idpel}&blth={blth_lalu2}',
        }

    return render_template('cek_foto.html',
                           links=links,
                           idpel=idpel,
                           blth_kini=blth_kini,
                           blth_lalu=blth_lalu,
                           blth_lalu2=blth_lalu2)


    
    
    
    
    
def save_to_database(df, table_name):
    try:
        # Simpan DataFrame ke tabel MySQL
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Data berhasil disimpan ke database 'billing' di tabel '{table_name}'")
    except Exception as e:
        print(f"Terjadi kesalahan saat menyimpan data ke database 'billing': {e}")


def save_to_database2(df, table_name):
    try:
        # Simpan DataFrame ke tabel MySQL
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Data berhasil disimpan ke database 'billing2' di tabel '{table_name}'")
    except Exception as e:
        print(f"Terjadi kesalahan saat menyimpan data ke database 'billing2': {e}")


def save_to_database3(df, table_name):
    try:
        # Simpan DataFrame ke tabel MySQL
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Data berhasil disimpan ke database 'billing2' di tabel '{table_name}'")
    except Exception as e:
        print(f"Terjadi kesalahan saat menyimpan data ke database 'billing2': {e}")


def save_to_database4(df, table_name):
    try:
        # Simpan DataFrame ke tabel MySQL
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Data berhasil disimpan ke database 'billing2' di tabel '{table_name}'")
    except Exception as e:
        print(f"Terjadi kesalahan saat menyimpan data ke database 'billing2': {e}")
        
def save_to_database5(df, table_name):
    try:
        # Simpan DataFrame ke tabel MySQL
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Data berhasil disimpan ke database 'billing2' di tabel '{table_name}'")
    except Exception as e:
        print(f"Terjadi kesalahan saat menyimpan data ke database 'billing2': {e}")        
    
def save_to_database6(df, table_name):
    try:
        # Simpan DataFrame ke tabel MySQL
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Data berhasil disimpan ke database 'billing2' di tabel '{table_name}'")
    except Exception as e:
        print(f"Terjadi kesalahan saat menyimpan data ke database 'billing2': {e}") 

def save_to_database7(df, table_name):
    try:
        # Simpan DataFrame ke tabel MySQL
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Data berhasil disimpan ke database 'billing2' di tabel '{table_name}'")
    except Exception as e:
        print(f"Terjadi kesalahan saat menyimpan data ke database 'billing2': {e}") 
        
def save_to_database8(df, table_name):
    try:
        # Simpan DataFrame ke tabel MySQL
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Data berhasil disimpan ke database 'billing2' di tabel '{table_name}'")
    except Exception as e:
        print(f"Terjadi kesalahan saat menyimpan data ke database 'billing2': {e}")        

def save_to_database9(df, table_name):
    try:
        # Simpan DataFrame ke tabel MySQL
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Data berhasil disimpan ke database 'billing2' di tabel '{table_name}'")
    except Exception as e:
        print(f"Terjadi kesalahan saat menyimpan data ke database 'billing2': {e}") 
        
def save_to_database10(df, table_name):
    try:
        # Simpan DataFrame ke tabel MySQL
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Data berhasil disimpan ke database 'billing2' di tabel '{table_name}'")
    except Exception as e:
        print(f"Terjadi kesalahan saat menyimpan data ke database 'billing2': {e}") 
        
#std
@app.route("/save_to_db", methods=["POST"])
def save_to_db():
    global naik_df, turun_df, div_df  # Pastikan DataFrame global bisa diakses

    try:
        # Simpan DataFrame ke database 'billing'
        save_to_database(naik_df, 'billing_naik')
        save_to_database(turun_df, 'billing_turun')
        save_to_database(div_df, 'billing_div')
        return {"status": "success", "message": "Data berhasil disimpan ke database 'billing'"}
    except Exception as e:
        return {"status": "error", "message": f"Terjadi kesalahan: {e}"}

#####
@app.route("/save_to_db2", methods=["POST"])
def save_to_db2():
    global naik_df, turun_df, div_df  # Pastikan DataFrame global bisa diakses

    try:
        # Simpan DataFrame ke database 'billing2'
        save_to_database2(naik_df, 'billing_naik2')
        save_to_database2(turun_df, 'billing_turun2')
        save_to_database2(div_df, 'billing_div2')
        return {"status": "success", "message": "Data berhasil disimpan ke database 'billing2'"}
    except Exception as e:
        return {"status": "error", "message": f"Terjadi kesalahan: {e}"}

@app.route("/save_to_db3", methods=["POST"])
def save_to_db3():
    global naik_df, turun_df, div_df  # Pastikan DataFrame global bisa diakses

    try:
        # Simpan DataFrame ke database 'billing2'
        save_to_database2(naik_df, 'billing_naik3')
        save_to_database2(turun_df, 'billing_turun3')
        save_to_database2(div_df, 'billing_div3')
        return {"status": "success", "message": "Data berhasil disimpan ke database 'billing2'"}
    except Exception as e:
        return {"status": "error", "message": f"Terjadi kesalahan: {e}"}


@app.route("/save_to_db4", methods=["POST"])
def save_to_db4():
    global naik_df, turun_df, div_df  # Pastikan DataFrame global bisa diakses

    try:
        # Simpan DataFrame ke database 'billing2'
        save_to_database2(naik_df, 'billing_naik4')
        save_to_database2(turun_df, 'billing_turun4')
        save_to_database2(div_df, 'billing_div4')
        return {"status": "success", "message": "Data berhasil disimpan ke database 'billing2'"}
    except Exception as e:
        return {"status": "error", "message": f"Terjadi kesalahan: {e}"}

@app.route("/save_to_db5", methods=["POST"])
def save_to_db5():
    global naik_df, turun_df, div_df  # Pastikan DataFrame global bisa diakses

    try:
        # Simpan DataFrame ke database 'billing2'
        save_to_database2(naik_df, 'billing_naik5')
        save_to_database2(turun_df, 'billing_turun5')
        save_to_database2(div_df, 'billing_div5')
        return {"status": "success", "message": "Data berhasil disimpan ke database 'billing2'"}
    except Exception as e:
        return {"status": "error", "message": f"Terjadi kesalahan: {e}"}


@app.route("/save_to_db6", methods=["POST"])
def save_to_db6():
    global naik_df, turun_df, div_df  # Pastikan DataFrame global bisa diakses

    try:
        # Simpan DataFrame ke database 'billing2'
        save_to_database2(naik_df, 'billing_naik6')
        save_to_database2(turun_df, 'billing_turun6')
        save_to_database2(div_df, 'billing_div6')
        return {"status": "success", "message": "Data berhasil disimpan ke database 'billing2'"}
    except Exception as e:
        return {"status": "error", "message": f"Terjadi kesalahan: {e}"}

@app.route("/save_to_db7", methods=["POST"])
def save_to_db7():
    global naik_df, turun_df, div_df  # Pastikan DataFrame global bisa diakses

    try:
        # Simpan DataFrame ke database 'billing2'
        save_to_database2(naik_df, 'billing_naik7')
        save_to_database2(turun_df, 'billing_turun7')
        save_to_database2(div_df, 'billing_div7')
        return {"status": "success", "message": "Data berhasil disimpan ke database 'billing2'"}
    except Exception as e:
        return {"status": "error", "message": f"Terjadi kesalahan: {e}"}

@app.route("/save_to_db8", methods=["POST"])
def save_to_db8():
    global naik_df, turun_df, div_df  # Pastikan DataFrame global bisa diakses

    try:
        # Simpan DataFrame ke database 'billing2'
        save_to_database2(naik_df, 'billing_naik8')
        save_to_database2(turun_df, 'billing_turun8')
        save_to_database2(div_df, 'billing_div8')
        return {"status": "success", "message": "Data berhasil disimpan ke database 'billing2'"}
    except Exception as e:
        return {"status": "error", "message": f"Terjadi kesalahan: {e}"}


@app.route("/save_to_db9", methods=["POST"])
def save_to_db9():
    global naik_df, turun_df, div_df  # Pastikan DataFrame global bisa diakses

    try:
        # Simpan DataFrame ke database 'billing2'
        save_to_database2(naik_df, 'billing_naik9')
        save_to_database2(turun_df, 'billing_turun9')
        save_to_database2(div_df, 'billing_div9')
        return {"status": "success", "message": "Data berhasil disimpan ke database 'billing2'"}
    except Exception as e:
        return {"status": "error", "message": f"Terjadi kesalahan: {e}"}
    
    
@app.route("/save_to_db10", methods=["POST"])
def save_to_db10():
    global naik_df, turun_df, div_df  # Pastikan DataFrame global bisa diakses

    try:
        # Simpan DataFrame ke database 'billing2'
        save_to_database2(naik_df, 'billing_naik10')
        save_to_database2(turun_df, 'billing_turun10')
        save_to_database2(div_df, 'billing_div10')
        return {"status": "success", "message": "Data berhasil disimpan ke database 'billing2'"}
    except Exception as e:
        return {"status": "error", "message": f"Terjadi kesalahan: {e}"}
    
    
# Global DataFrames to store results
# result_df = None
naik_df = None
turun_df = None
# aman_df = None
div_df = None
#################################################################################
@app.route('/admin_ulp', methods=['POST'])
@login_required
def admin_ulp():
    ulp = request.form.get('ulp')
    session['ulp'] = ulp  # Simpan pilihan ULP di session

    if ulp == "ULP MEMPAWAH":
        return redirect(url_for('admin_mempawah'))
    elif ulp == "ULP SIANTAN":
        return redirect(url_for('admin_siantan'))
    elif ulp == "ULP NGABANG":
        return redirect(url_for('admin_ngabang'))
    elif ulp == "ULP TANJUNG RAYA":
        return redirect(url_for('admin_tanjungraya'))
    elif ulp == "UP3 MEMPAWAH":
        return redirect(url_for('admin_up3'))
    else:
        flash('ULP tidak dikenal', 'danger')
        return redirect(url_for('login'))


@app.route('/admin_mempawah')
@login_required
def admin_mempawah():
    return render_template('admin_mempawah.html', ulp="ULP MEMPAWAH")

@app.route('/admin_siantan')
@login_required
def admin_siantan():
    return render_template('admin_siantan.html', ulp="ULP SIANTAN")

@app.route('/admin_ngabang')
@login_required
def admin_ngabang():
    return render_template('admin_ngabang.html', ulp="ULP NGABANG")

@app.route('/admin_tanjungraya')
@login_required
def admin_tanjungraya():
    return render_template('admin_tanjungraya.html', ulp="ULP TANJUNG RAYA")

@app.route('/admin_up3')
@login_required
def admin_up3():
    return render_template('admin_up3.html', ulp="UP3 MEMPAWAH")

##############################################################################
data_cache = {}
@app.route('/foto', methods=['GET', 'POST'])
def kelola_foto():
    data = None
    if request.method == 'POST':
        file = request.files['file']
        form1 = request.form.get('form1')  # FOTO AKHIR
        form2 = request.form.get('form2')  # FOTO LALU
        form3 = request.form.get('form3')  # FOTO LALU2

        if file and form1 and form2 and form3:
            df = pd.read_excel(file)
            df['BLTH'] = df['BLTH'].astype(str)

            path_foto = 'https://portalapp.iconpln.co.id/acmt/DisplayBlobServlet1?idpel='

            for i in range(len(df)):
                idpel = df.loc[i, 'IDPEL']

                for col, blth in zip(['FOTO AKHIR', 'FOTO LALU', 'FOTO LALU2'], [form1, form2, form3]):
                    df.loc[i, col] = (
                        f'<a href="{path_foto}{idpel}&blth={blth}" target="popup" '
                        f'onclick="window.open(\'{path_foto}{idpel}&blth={blth}\', '
                        f'\'popup\', \'width=700,height=700,scrollbars=no,toolbar=no\'); return false;">LINK FOTO</a>'
                    )

            df['SLALWBP'] = df['SLALWBP'].fillna(0).astype(int)
            df['LWBPPASANG'] = df['LWBPPASANG'].fillna(0).astype(int)
            df['LWBPCABUT'] = df['LWBPCABUT'].fillna(0).astype(int)
            df['SAHLWBP'] = df['SAHLWBP'].fillna(0).astype(int)

            # Tambahkan kolom dropdown dan textarea
            def generate_dropdown(x=None):
                options = [
                    "SESUAI", "SALAH STAN", "TELAT/SALAH PDL", "SALAH FOTO",
                    "FOTO BURAM", "LEBIH TAGIH", "BUKAN FOTO KWH", "BENCANA"
                ]
                dropdown = (
                    '<select class="hasil-pemeriksaan" '
                    'onfocus="this.options[0].selected = true;">'
                    '<option value="" disabled selected hidden></option>'
                )
                for opt in options:
                    selected = ' selected' if x == opt else ''
                    dropdown += f'<option value="{opt}"{selected}>{opt}</option>'
                dropdown += '</select>'
                return dropdown

            df['HASIL PEMERIKSAAN'] = df.apply(lambda x: generate_dropdown(), axis=1)
            df['KETERANGAN'] = '<textarea class="tindak-lanjut" rows="2" cols="30"></textarea>'

            columns_to_display = [
                'BLTH', 'IDPEL', 'NAMA', 'TARIF', 'DAYA', 'SLALWBP',
                'LWBPCABUT', 'LWBPPASANG', 'LWBPPAKAI', 'SAHLWBP',
                'FOTO AKHIR', 'FOTO LALU', 'FOTO LALU2',
                'HASIL PEMERIKSAAN', 'KETERANGAN'
            ]
            df = df[columns_to_display]

            # Simpan ke database (tanpa kolom verifikasi & keterangan karena hanya untuk tampilan)
            import mysql.connector
            conn = mysql.connector.connect(
                host='localhost', user='root', password='', database='telupsin_billing2'
            )
            cursor = conn.cursor()
            cursor.execute("DELETE FROM foto_data")

            for _, row in df.iterrows():
                sql = """
                INSERT INTO foto_data (
                    BLTH, IDPEL, NAMA, TARIF, DAYA, SLALWBP,
                    LWBPCABUT, LWBPPASANG, LWBPPAKAI, SAHLWBP,
                    FOTO_AKHIR, FOTO_LALU, FOTO_LALU2
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                row = row.fillna(0)
                values = (
                    row['BLTH'], row['IDPEL'], row['NAMA'], row['TARIF'], row['DAYA'], row['SLALWBP'],
                    row['LWBPCABUT'], row['LWBPPASANG'], row['LWBPPAKAI'], row['SAHLWBP'],
                    row['FOTO AKHIR'], row['FOTO LALU'], row['FOTO LALU2']
                )
                cursor.execute(sql, values)

            conn.commit()
            cursor.close()
            conn.close()

            data = df.to_dict(orient='records')

    return render_template('foto.html', data=data)



# @app.route("/download_foto_xlsx")
# def download_foto_xlsx():
#     from io import BytesIO
#     import pandas as pd

#     # Ambil ulang data dari sumber (misal database atau session)
#     df = get_your_data_source()

#     # Buat link klikable (opsional)
#     for col in ["FOTO AKHIR", "FOTO LALU", "FOTO LALU2"]:
#         if col in df.columns:
#             df[col] = df[col].apply(lambda x: f'=HYPERLINK("{x}", "LINK FOTO")' if pd.notnull(x) else "")

#     output = BytesIO()
#     with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
#         df.to_excel(writer, index=False, sheet_name="Sheet1")
#         writer.save()
#     output.seek(0)

#     return send_file(output, download_name="Data_Foto_UP3.xlsx", as_attachment=True)
#######################################################################
@app.route('/update_hasil', methods=['POST'])
def update_hasil():
    idpel = request.form.get('idpel')
    hasil = request.form.get('hasil')

    conn = mysql.connection  # ✅ Tanpa tanda kurung
    cursor = conn.cursor()
    cursor.execute("UPDATE foto_data SET hasil_pemeriksaan = %s WHERE idpel = %s", (hasil, idpel))
    conn.commit()
    cursor.close()
    return jsonify({'status': 'ok'})


@app.route('/update_keterangan', methods=['POST'])
def update_keterangan():
    idpel = request.form.get('idpel')
    keterangan = request.form.get('keterangan')

    conn = mysql.connection  # ✅ Tanpa tanda kurung
    cursor = conn.cursor()
    cursor.execute("UPDATE foto_data SET keterangan = %s WHERE idpel = %s", (keterangan, idpel))
    conn.commit()
    cursor.close()
    return jsonify({'status': 'ok'})
#########################################################################


@app.route('/download_foto_data')
def download_foto_data():
    import mysql.connector

    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='',
        database='telupsin_billing2'
    )

    query = "SELECT * FROM foto_data"
    df = pd.read_sql(query, conn)
    conn.close()

    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='foto_data')
    output.seek(0)

    return send_file(
        output,
        as_attachment=True,
        download_name='foto_data.xlsx',
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )





#####################################################################






###################################################################
@app.route("/1", methods=["GET", "POST"])
@login_required
def index1():
    
    # if "username" not in session:
    #     return redirect(url_for("login"))
    
    global naik_df, turun_df, div_df  # Use global to access DataFrames across routes

    if request.method == "POST":
        blth_lalulalu = request.form['blth_lalulalu']
        blth_lalu = request.form['blth_lalu']
        blth_kini = request.form['blth_kini']

        file_lalulalu = request.files['file_lalulalu']
        file_lalu = request.files['file_lalu']
        file_akhir = request.files['file_akhir']

        if file_lalulalu and file_lalu and file_akhir:
            lalulalu = pd.read_excel(file_lalulalu)
            lalu = pd.read_excel(file_lalu)
            akhir = pd.read_excel(file_akhir)

            
            # Filter data
            naik_df = naikFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
            turun_df = turunFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
            # aman_df = amanFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
            div_df = divFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)

            # # Simpan DataFrame ke MySQL
            # save_to_database(result_df, 'billing_result')
            save_to_database(naik_df, 'billing_naik')
            save_to_database(turun_df, 'billing_turun')
            # save_to_database(aman_df, 'billing_aman')
            save_to_database(div_df, 'billing_div')

            # Convert DataFrames to HTML
            # result_html = result_df.to_html(escape=False)
            naik_html = naik_df.to_html(escape=False)
            turun_html = turun_df.to_html(escape=False)
            # aman_html = aman_df.to_html(escape=False)
            div_html = div_df.to_html(escape=False)

            return render_template('index1.html',  naik=naik_html, turun=turun_html,  div=div_html)

    return render_template('index1.html',  naik=None, turun=None, div=None)


@app.route("/2", methods=["GET", "POST"])
@login_required
def index2():
    
    # if "username" not in session:
    #     return redirect(url_for("login"))
    
    global naik_df, turun_df, div_df  # Use global to access DataFrames across routes

    if request.method == "POST":
        blth_lalulalu = request.form['blth_lalulalu']
        blth_lalu = request.form['blth_lalu']
        blth_kini = request.form['blth_kini']

        file_lalulalu = request.files['file_lalulalu']
        file_lalu = request.files['file_lalu']
        file_akhir = request.files['file_akhir']

        if file_lalulalu and file_lalu and file_akhir:
            lalulalu = pd.read_excel(file_lalulalu)
            lalu = pd.read_excel(file_lalu)
            akhir = pd.read_excel(file_akhir)


            
            # Filter data
            naik_df = naikFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
            turun_df = turunFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
            # aman_df = amanFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
            div_df = divFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)

            # # Simpan DataFrame ke MySQL
            # save_to_database(result_df, 'billing_result')
            save_to_database2(naik_df, 'billing_naik2')
            save_to_database2(turun_df, 'billing_turun2')
            # save_to_database(aman_df, 'billing_aman')
            save_to_database2(div_df, 'billing_div2')

            # Convert DataFrames to HTML
            # result_html = result_df.to_html(escape=False)
            naik_html = naik_df.to_html(escape=False)
            turun_html = turun_df.to_html(escape=False)
            # aman_html = aman_df.to_html(escape=False)
            div_html = div_df.to_html(escape=False)

            return render_template('index2.html',  naik=naik_html, turun=turun_html,  div=div_html)

    return render_template('index2.html',  naik=None, turun=None, div=None)


@app.route("/3", methods=["GET", "POST"])
@login_required
def index3():
    
    # if "username" not in session:
    #     return redirect(url_for("login"))
    
    global naik_df, turun_df, div_df  # Use global to access DataFrames across routes

    if request.method == "POST":
        blth_lalulalu = request.form['blth_lalulalu']
        blth_lalu = request.form['blth_lalu']
        blth_kini = request.form['blth_kini']

        file_lalulalu = request.files['file_lalulalu']
        file_lalu = request.files['file_lalu']
        file_akhir = request.files['file_akhir']

        if file_lalulalu and file_lalu and file_akhir:
            lalulalu = pd.read_excel(file_lalulalu)
            lalu = pd.read_excel(file_lalu)
            akhir = pd.read_excel(file_akhir)

            
            # Filter data
            naik_df = naikFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
            turun_df = turunFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
            # aman_df = amanFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
            div_df = divFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)

            # # Simpan DataFrame ke MySQL
            # save_to_database(result_df, 'billing_result')
            save_to_database2(naik_df, 'billing_naik3')
            save_to_database2(turun_df, 'billing_turun3')
            # save_to_database(aman_df, 'billing_aman')
            save_to_database2(div_df, 'billing_div3')

            # Convert DataFrames to HTML
            # result_html = result_df.to_html(escape=False)
            naik_html = naik_df.to_html(escape=False)
            turun_html = turun_df.to_html(escape=False)
            # aman_html = aman_df.to_html(escape=False)
            div_html = div_df.to_html(escape=False)

            return render_template('index3.html',  naik=naik_html, turun=turun_html,  div=div_html)

    return render_template('index3.html',  naik=None, turun=None, div=None)


@app.route("/4", methods=["GET", "POST"])
@login_required
def index4():
    
    # if "username" not in session:
    #     return redirect(url_for("login"))
    
    global naik_df, turun_df, div_df  # Use global to access DataFrames across routes

    if request.method == "POST":
        blth_lalulalu = request.form['blth_lalulalu']
        blth_lalu = request.form['blth_lalu']
        blth_kini = request.form['blth_kini']

        file_lalulalu = request.files['file_lalulalu']
        file_lalu = request.files['file_lalu']
        file_akhir = request.files['file_akhir']

        if file_lalulalu and file_lalu and file_akhir:
            lalulalu = pd.read_excel(file_lalulalu)
            lalu = pd.read_excel(file_lalu)
            akhir = pd.read_excel(file_akhir)


            
            # Filter data
            naik_df = naikFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
            turun_df = turunFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
            # aman_df = amanFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
            div_df = divFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)

            # # Simpan DataFrame ke MySQL
            # save_to_database(result_df, 'billing_result')
            save_to_database2(naik_df, 'billing_naik4')
            save_to_database2(turun_df, 'billing_turun4')
            # save_to_database(aman_df, 'billing_aman')
            save_to_database2(div_df, 'billing_div4')

            # Convert DataFrames to HTML
            # result_html = result_df.to_html(escape=False)
            naik_html = naik_df.to_html(escape=False)
            turun_html = turun_df.to_html(escape=False)
            # aman_html = aman_df.to_html(escape=False)
            div_html = div_df.to_html(escape=False)

            return render_template('index4.html',  naik=naik_html, turun=turun_html,  div=div_html)

    return render_template('index4.html',  naik=None, turun=None, div=None)


@app.route("/5", methods=["GET", "POST"])
@login_required
def index5():
    
    # if "username" not in session:
    #     return redirect(url_for("login"))
    
    global naik_df, turun_df, div_df  # Use global to access DataFrames across routes

    if request.method == "POST":
        blth_lalulalu = request.form['blth_lalulalu']
        blth_lalu = request.form['blth_lalu']
        blth_kini = request.form['blth_kini']

        file_lalulalu = request.files['file_lalulalu']
        file_lalu = request.files['file_lalu']
        file_akhir = request.files['file_akhir']

        if file_lalulalu and file_lalu and file_akhir:
            lalulalu = pd.read_excel(file_lalulalu)
            lalu = pd.read_excel(file_lalu)
            akhir = pd.read_excel(file_akhir)

            
            # Filter data
            naik_df = naikFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
            turun_df = turunFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
            # aman_df = amanFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
            div_df = divFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)

            # # Simpan DataFrame ke MySQL
            # save_to_database(result_df, 'billing_result')
            save_to_database2(naik_df, 'billing_naik5')
            save_to_database2(turun_df, 'billing_turun5')
            # save_to_database(aman_df, 'billing_aman')
            save_to_database2(div_df, 'billing_div5')

            # Convert DataFrames to HTML
            # result_html = result_df.to_html(escape=False)
            naik_html = naik_df.to_html(escape=False)
            turun_html = turun_df.to_html(escape=False)
            # aman_html = aman_df.to_html(escape=False)
            div_html = div_df.to_html(escape=False)

            return render_template('index5.html',  naik=naik_html, turun=turun_html,  div=div_html)

    return render_template('index5.html',  naik=None, turun=None, div=None)

@app.route("/6", methods=["GET", "POST"])
@login_required
def index6():
    
    # if "username" not in session:
    #     return redirect(url_for("login"))
    
    global naik_df, turun_df, div_df  # Use global to access DataFrames across routes

    if request.method == "POST":
        blth_lalulalu = request.form['blth_lalulalu']
        blth_lalu = request.form['blth_lalu']
        blth_kini = request.form['blth_kini']

        file_lalulalu = request.files['file_lalulalu']
        file_lalu = request.files['file_lalu']
        file_akhir = request.files['file_akhir']

        if file_lalulalu and file_lalu and file_akhir:
            lalulalu = pd.read_excel(file_lalulalu)
            lalu = pd.read_excel(file_lalu)
            akhir = pd.read_excel(file_akhir)


            
            # Filter data
            naik_df = naikFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
            turun_df = turunFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
            # aman_df = amanFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
            div_df = divFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)

            # # Simpan DataFrame ke MySQL
            # save_to_database(result_df, 'billing_result')
            save_to_database2(naik_df, 'billing_naik6')
            save_to_database2(turun_df, 'billing_turun6')
            # save_to_database(aman_df, 'billing_aman')
            save_to_database2(div_df, 'billing_div6')

            # Convert DataFrames to HTML
            # result_html = result_df.to_html(escape=False)
            naik_html = naik_df.to_html(escape=False)
            turun_html = turun_df.to_html(escape=False)
            # aman_html = aman_df.to_html(escape=False)
            div_html = div_df.to_html(escape=False)

            return render_template('index6.html',  naik=naik_html, turun=turun_html,  div=div_html)

    return render_template('index6.html',  naik=None, turun=None, div=None)

@app.route("/7", methods=["GET", "POST"])
@login_required
def index7():
    
    # if "username" not in session:
    #     return redirect(url_for("login"))
    
    global naik_df, turun_df, div_df  # Use global to access DataFrames across routes

    if request.method == "POST":
        blth_lalulalu = request.form['blth_lalulalu']
        blth_lalu = request.form['blth_lalu']
        blth_kini = request.form['blth_kini']

        file_lalulalu = request.files['file_lalulalu']
        file_lalu = request.files['file_lalu']
        file_akhir = request.files['file_akhir']

        if file_lalulalu and file_lalu and file_akhir:
            lalulalu = pd.read_excel(file_lalulalu)
            lalu = pd.read_excel(file_lalu)
            akhir = pd.read_excel(file_akhir)


            
            # Filter data
            naik_df = naikFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
            turun_df = turunFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
            # aman_df = amanFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
            div_df = divFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)

            # # Simpan DataFrame ke MySQL
            # save_to_database(result_df, 'billing_result')
            save_to_database2(naik_df, 'billing_naik7')
            save_to_database2(turun_df, 'billing_turun7')
            # save_to_database(aman_df, 'billing_aman')
            save_to_database2(div_df, 'billing_div7')

            # Convert DataFrames to HTML
            # result_html = result_df.to_html(escape=False)
            naik_html = naik_df.to_html(escape=False)
            turun_html = turun_df.to_html(escape=False)
            # aman_html = aman_df.to_html(escape=False)
            div_html = div_df.to_html(escape=False)

            return render_template('index7.html',  naik=naik_html, turun=turun_html,  div=div_html)


    return render_template('index7.html',  naik=None, turun=None, div=None)

@app.route("/8", methods=["GET", "POST"])
@login_required
def index8():
    
    # if "username" not in session:
    #     return redirect(url_for("login"))
    
    global naik_df, turun_df, div_df  # Use global to access DataFrames across routes

    if request.method == "POST":
        blth_lalulalu = request.form['blth_lalulalu']
        blth_lalu = request.form['blth_lalu']
        blth_kini = request.form['blth_kini']

        file_lalulalu = request.files['file_lalulalu']
        file_lalu = request.files['file_lalu']
        file_akhir = request.files['file_akhir']

        if file_lalulalu and file_lalu and file_akhir:
            lalulalu = pd.read_excel(file_lalulalu)
            lalu = pd.read_excel(file_lalu)
            akhir = pd.read_excel(file_akhir)


            
            # Filter data
            naik_df = naikFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
            turun_df = turunFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
            # aman_df = amanFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
            div_df = divFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)

            # # Simpan DataFrame ke MySQL
            # save_to_database(result_df, 'billing_result')
            save_to_database2(naik_df, 'billing_naik8')
            save_to_database2(turun_df, 'billing_turun8')
            # save_to_database(aman_df, 'billing_aman')
            save_to_database2(div_df, 'billing_div8')

            # Convert DataFrames to HTML
            # result_html = result_df.to_html(escape=False)
            naik_html = naik_df.to_html(escape=False)
            turun_html = turun_df.to_html(escape=False)
            # aman_html = aman_df.to_html(escape=False)
            div_html = div_df.to_html(escape=False)

            return render_template('index8.html',  naik=naik_html, turun=turun_html,  div=div_html)


    return render_template('index8.html',  naik=None, turun=None, div=None)


@app.route("/9", methods=["GET", "POST"])
@login_required
def index9():
    
    # if "username" not in session:
    #     return redirect(url_for("login"))
    
    global naik_df, turun_df, div_df  # Use global to access DataFrames across routes

    if request.method == "POST":
        blth_lalulalu = request.form['blth_lalulalu']
        blth_lalu = request.form['blth_lalu']
        blth_kini = request.form['blth_kini']

        file_lalulalu = request.files['file_lalulalu']
        file_lalu = request.files['file_lalu']
        file_akhir = request.files['file_akhir']

        if file_lalulalu and file_lalu and file_akhir:
            lalulalu = pd.read_excel(file_lalulalu)
            lalu = pd.read_excel(file_lalu)
            akhir = pd.read_excel(file_akhir)


            
            # Filter data
            naik_df = naikFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
            turun_df = turunFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
            # aman_df = amanFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
            div_df = divFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)

            # # Simpan DataFrame ke MySQL
            # save_to_database(result_df, 'billing_result')
            save_to_database2(naik_df, 'billing_naik9')
            save_to_database2(turun_df, 'billing_turun9')
            # save_to_database(aman_df, 'billing_aman')
            save_to_database2(div_df, 'billing_div9')

            # Convert DataFrames to HTML
            # result_html = result_df.to_html(escape=False)
            naik_html = naik_df.to_html(escape=False)
            turun_html = turun_df.to_html(escape=False)
            # aman_html = aman_df.to_html(escape=False)
            div_html = div_df.to_html(escape=False)

            return render_template('index9.html',  naik=naik_html, turun=turun_html,  div=div_html)


    return render_template('index9.html',  naik=None, turun=None, div=None)


@app.route("/10", methods=["GET", "POST"])
@login_required
def index10():
    
    # if "username" not in session:
    #     return redirect(url_for("login"))
    
    global naik_df, turun_df, div_df  # Use global to access DataFrames across routes

    if request.method == "POST":
        blth_lalulalu = request.form['blth_lalulalu']
        blth_lalu = request.form['blth_lalu']
        blth_kini = request.form['blth_kini']

        file_lalulalu = request.files['file_lalulalu']
        file_lalu = request.files['file_lalu']
        file_akhir = request.files['file_akhir']

        if file_lalulalu and file_lalu and file_akhir:
            lalulalu = pd.read_excel(file_lalulalu)
            lalu = pd.read_excel(file_lalu)
            akhir = pd.read_excel(file_akhir)


            
            # Filter data
            naik_df = naikFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
            turun_df = turunFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
            # aman_df = amanFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
            div_df = divFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)

            # # Simpan DataFrame ke MySQL
            # save_to_database(result_df, 'billing_result')
            save_to_database2(naik_df, 'billing_naik10')
            save_to_database2(turun_df, 'billing_turun10')
            # save_to_database(aman_df, 'billing_aman')
            save_to_database2(div_df, 'billing_div10')

            # Convert DataFrames to HTML
            # result_html = result_df.to_html(escape=False)
            naik_html = naik_df.to_html(escape=False)
            turun_html = turun_df.to_html(escape=False)
            # aman_html = aman_df.to_html(escape=False)
            div_html = div_df.to_html(escape=False)

            return render_template('index10.html',  naik=naik_html, turun=turun_html,  div=div_html)


    return render_template('index10.html',  naik=None, turun=None, div=None)

# View Data (Proteksi login ditambahkan)
@app.route("/view_data1")
@login_required
def view_data1():
    print("Database URL:")

    # Query data dari tabel billing_result, billing_naik, billing_turun, dan billing_aman
    # data_result = pd.read_sql("SELECT * FROM billing_result", engine)
    data_naik = pd.read_sql("SELECT * FROM billing_naik", engine)
    data_turun = pd.read_sql("SELECT * FROM billing_turun",engine)
    # data_aman = pd.read_sql("SELECT * FROM billing_aman", engine)
    data_div = pd.read_sql("SELECT * FROM billing_div", engine)
    

    # Konversi DataFrames ke HTML
    # result_html = data_result.to_html(classes="table table-striped", index=False, escape=False)
    naik_html = data_naik.to_html(classes="table table-striped", index=False, escape=False)
    turun_html = data_turun.to_html(classes="table table-striped", index=False, escape=False)
    # aman_html = data_aman.to_html(classes="table table-striped", index=False, escape=False)
    div_html = data_div.to_html(classes="table table-striped", index=False, escape=False)

    # Render template view_data.html dengan data HTML yang telah dikonversi
    return render_template(
        "view_data1.html",
        # result_html=result_html,
        naik_html=naik_html,
        turun_html=turun_html,
        # aman_html=aman_html,
        div_html=div_html
    

    )
  


# View Data (Proteksi login ditambahkan)
# View Data untuk database kedua (billing2)
@app.route("/view_data2")
@login_required
def view_data2():
    # Query data dari tabel billing_naik2, billing_turun2, dan billing_div2 di database kedua
    data_naik = pd.read_sql("SELECT * FROM billing_naik2", engine)
    data_turun = pd.read_sql("SELECT * FROM billing_turun2", engine)
    data_div = pd.read_sql("SELECT * FROM billing_div2", engine)
    
    # Konversi DataFrames ke HTML
    naik_html = data_naik.to_html(classes="table table-striped", index=False, escape=False)
    turun_html = data_turun.to_html(classes="table table-striped", index=False, escape=False)
    div_html = data_div.to_html(classes="table table-striped", index=False, escape=False)

    # Render template view_data2.html dengan data HTML
    return render_template(
        "view_data2.html",
        naik_html=naik_html,
        turun_html=turun_html,
        div_html=div_html
    )

@app.route("/view_data3")
@login_required
def view_data3():
    # Query data dari tabel billing_naik2, billing_turun2, dan billing_div2 di database kedua
    data_naik = pd.read_sql("SELECT * FROM billing_naik3", engine)
    data_turun = pd.read_sql("SELECT * FROM billing_turun3", engine)
    data_div = pd.read_sql("SELECT * FROM billing_div3", engine)
    
    # Konversi DataFrames ke HTML
    naik_html = data_naik.to_html(classes="table table-striped", index=False, escape=False)
    turun_html = data_turun.to_html(classes="table table-striped", index=False, escape=False)
    div_html = data_div.to_html(classes="table table-striped", index=False, escape=False)

    # Render template view_data2.html dengan data HTML
    return render_template(
        "view_data3.html",
        naik_html=naik_html,
        turun_html=turun_html,
        div_html=div_html
    )

@app.route("/view_data4")
@login_required
def view_data4():
    # Query data dari tabel billing_naik2, billing_turun2, dan billing_div2 di database kedua
    data_naik = pd.read_sql("SELECT * FROM billing_naik4", engine)
    data_turun = pd.read_sql("SELECT * FROM billing_turun4", engine)
    data_div = pd.read_sql("SELECT * FROM billing_div4", engine)
    
    # Konversi DataFrames ke HTML
    naik_html = data_naik.to_html(classes="table table-striped", index=False, escape=False)
    turun_html = data_turun.to_html(classes="table table-striped", index=False, escape=False)
    div_html = data_div.to_html(classes="table table-striped", index=False, escape=False)

    # Render template view_data2.html dengan data HTML
    return render_template(
        "view_data4.html",
        naik_html=naik_html,
        turun_html=turun_html,
        div_html=div_html
    )
@app.route("/view_data5")
@login_required
def view_data5():
    # Query data dari tabel billing_naik2, billing_turun2, dan billing_div2 di database kedua
    data_naik = pd.read_sql("SELECT * FROM billing_naik5", engine)
    data_turun = pd.read_sql("SELECT * FROM billing_turun5", engine)
    data_div = pd.read_sql("SELECT * FROM billing_div5", engine)
    
    # Konversi DataFrames ke HTML
    naik_html = data_naik.to_html(classes="table table-striped", index=False, escape=False)
    turun_html = data_turun.to_html(classes="table table-striped", index=False, escape=False)
    div_html = data_div.to_html(classes="table table-striped", index=False, escape=False)

    # Render template view_data2.html dengan data HTML
    return render_template(
        "view_data5.html",
        naik_html=naik_html,
        turun_html=turun_html,
        div_html=div_html
    )

@app.route("/view_data6")
@login_required
def view_data6():
    # Query data dari tabel billing_naik2, billing_turun2, dan billing_div2 di database kedua
    data_naik = pd.read_sql("SELECT * FROM billing_naik6", engine)
    data_turun = pd.read_sql("SELECT * FROM billing_turun6", engine)
    data_div = pd.read_sql("SELECT * FROM billing_div6", engine)
    
    # Konversi DataFrames ke HTML
    naik_html = data_naik.to_html(classes="table table-striped", index=False, escape=False)
    turun_html = data_turun.to_html(classes="table table-striped", index=False, escape=False)
    div_html = data_div.to_html(classes="table table-striped", index=False, escape=False)

    # Render template view_data2.html dengan data HTML
    return render_template(
        "view_data6.html",
        naik_html=naik_html,
        turun_html=turun_html,
        div_html=div_html
    )

@app.route("/view_data7")
@login_required
def view_data7():
    # Query data dari tabel billing_naik2, billing_turun2, dan billing_div2 di database kedua
    data_naik = pd.read_sql("SELECT * FROM billing_naik7", engine)
    data_turun = pd.read_sql("SELECT * FROM billing_turun7", engine)
    data_div = pd.read_sql("SELECT * FROM billing_div7", engine)
    
    # Konversi DataFrames ke HTML
    naik_html = data_naik.to_html(classes="table table-striped", index=False, escape=False)
    turun_html = data_turun.to_html(classes="table table-striped", index=False, escape=False)
    div_html = data_div.to_html(classes="table table-striped", index=False, escape=False)

    # Render template view_data2.html dengan data HTML
    return render_template(
        "view_data7.html",
        naik_html=naik_html,
        turun_html=turun_html,
        div_html=div_html
    )

@app.route("/view_data8")
@login_required
def view_data8():
    # Query data dari tabel billing_naik2, billing_turun2, dan billing_div2 di database kedua
    data_naik = pd.read_sql("SELECT * FROM billing_naik8", engine)
    data_turun = pd.read_sql("SELECT * FROM billing_turun8", engine)
    data_div = pd.read_sql("SELECT * FROM billing_div8", engine)
    
    # Konversi DataFrames ke HTML
    naik_html = data_naik.to_html(classes="table table-striped", index=False, escape=False)
    turun_html = data_turun.to_html(classes="table table-striped", index=False, escape=False)
    div_html = data_div.to_html(classes="table table-striped", index=False, escape=False)

    # Render template view_data2.html dengan data HTML
    return render_template(
        "view_data8.html",
        naik_html=naik_html,
        turun_html=turun_html,
        div_html=div_html
    )

@app.route("/view_data9")
@login_required
def view_data9():
    # Query data dari tabel billing_naik2, billing_turun2, dan billing_div2 di database kedua
    data_naik = pd.read_sql("SELECT * FROM billing_naik9", engine)
    data_turun = pd.read_sql("SELECT * FROM billing_turun9", engine)
    data_div = pd.read_sql("SELECT * FROM billing_div9", engine)
    
    # Konversi DataFrames ke HTML
    naik_html = data_naik.to_html(classes="table table-striped", index=False, escape=False)
    turun_html = data_turun.to_html(classes="table table-striped", index=False, escape=False)
    div_html = data_div.to_html(classes="table table-striped", index=False, escape=False)

    # Render template view_data2.html dengan data HTML
    return render_template(
        "view_data9.html",
        naik_html=naik_html,
        turun_html=turun_html,
        div_html=div_html
    )

@app.route("/view_data10")
@login_required
def view_data10():
    # Query data dari tabel billing_naik2, billing_turun2, dan billing_div2 di database kedua
    data_naik = pd.read_sql("SELECT * FROM billing_naik10", engine)
    data_turun = pd.read_sql("SELECT * FROM billing_turun10", engine)
    data_div = pd.read_sql("SELECT * FROM billing_div10", engine)
    
    # Konversi DataFrames ke HTML
    naik_html = data_naik.to_html(classes="table table-striped", index=False, escape=False)
    turun_html = data_turun.to_html(classes="table table-striped", index=False, escape=False)
    div_html = data_div.to_html(classes="table table-striped", index=False, escape=False)

    # Render template view_data2.html dengan data HTML
    return render_template(
        "view_data10.html",
        naik_html=naik_html,
        turun_html=turun_html,
        div_html=div_html
    )


@app.route('/1')
def main_dashboard():
    # Your main code to render data tables
    return render_template('view_data.html')

def get_db_connection():
    return mysql.connector.connect(**db_config)

@app.route('/download_data/<table>')
def download_data(table):
    # Query data from the specified table
    query = f"SELECT * FROM {table}"

    # Get data from the database
    data = pd.read_sql(query, engine)

    # Create Excel file in memory
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        data.to_excel(writer, index=False, sheet_name=table)
        # No need to call save() or close() explicitly; using 'with' handles it
    output.seek(0)

    # Send file
    return send_file(
        output,
        as_attachment=True,
        download_name=f"{table}_data.xlsx",
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
 
 
@app.route('/2')
def main_dashboard2():
    # Your main code to render data tables
    return render_template('view_data2.html')

def get_db_connection():
    return mysql.connector.connect(**db_config)

@app.route('/download_data2/<table>')
def download_data2(table):
    # Query data from the specified table
    query = f"SELECT * FROM {table}"

    # Get data from the database
    data = pd.read_sql(query, engine)

    # Create Excel file in memory
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        data.to_excel(writer, index=False, sheet_name=table)
        # No need to call save() or close() explicitly; using 'with' handles it
    output.seek(0)

    # Send file
    return send_file(
        output,
        as_attachment=True,
        download_name=f"{table}_data.xlsx",
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
     
@app.route('/3')
def main_dashboard3():
    # Your main code to render data tables
    return render_template('view_data3.html')

def get_db_connection():
    return mysql.connector.connect(**db_config)

@app.route('/download_data3/<table>')
def download_data3(table):
    # Query data from the specified table
    query = f"SELECT * FROM {table}"

    # Get data from the database
    data = pd.read_sql(query, engine)

    # Create Excel file in memory
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        data.to_excel(writer, index=False, sheet_name=table)
        # No need to call save() or close() explicitly; using 'with' handles it
    output.seek(0)

    # Send file
    return send_file(
        output,
        as_attachment=True,
        download_name=f"{table}_data.xlsx",
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    ) 

@app.route('/4')
def main_dashboard4():
    # Your main code to render data tables
    return render_template('view_data4.html')

def get_db_connection():
    return mysql.connector.connect(**db_config)

@app.route('/download_data4/<table>')
def download_data4(table):
    # Query data from the specified table
    query = f"SELECT * FROM {table}"

    # Get data from the database
    data = pd.read_sql(query, engine)

    # Create Excel file in memory
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        data.to_excel(writer, index=False, sheet_name=table)
        # No need to call save() or close() explicitly; using 'with' handles it
    output.seek(0)

    # Send file
    return send_file(
        output,
        as_attachment=True,
        download_name=f"{table}_data.xlsx",
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    ) 
 

@app.route('/5')
def main_dashboard5():
    # Your main code to render data tables
    return render_template('view_data5.html')

def get_db_connection():
    return mysql.connector.connect(**db_config)

@app.route('/download_data5/<table>')
def download_data5(table):
    # Query data from the specified table
    query = f"SELECT * FROM {table}"

    # Get data from the database
    data = pd.read_sql(query, engine)

    # Create Excel file in memory
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        data.to_excel(writer, index=False, sheet_name=table)
        # No need to call save() or close() explicitly; using 'with' handles it
    output.seek(0)

    # Send file
    return send_file(
        output,
        as_attachment=True,
        download_name=f"{table}_data.xlsx",
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )  

@app.route('/6')
def main_dashboard6():
    # Your main code to render data tables
    return render_template('view_data6.html')

def get_db_connection():
    return mysql.connector.connect(**db_config)

@app.route('/download_data6/<table>')
def download_data6(table):
    # Query data from the specified table
    query = f"SELECT * FROM {table}"

    # Get data from the database
    data = pd.read_sql(query, engine)

    # Create Excel file in memory
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        data.to_excel(writer, index=False, sheet_name=table)
        # No need to call save() or close() explicitly; using 'with' handles it
    output.seek(0)

    # Send file
    return send_file(
        output,
        as_attachment=True,
        download_name=f"{table}_data.xlsx",
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )  

@app.route('/7')
def main_dashboard7():
    # Your main code to render data tables
    return render_template('view_data7.html')

def get_db_connection():
    return mysql.connector.connect(**db_config)

@app.route('/download_data7/<table>')
def download_data7(table):
    # Query data from the specified table
    query = f"SELECT * FROM {table}"

    # Get data from the database
    data = pd.read_sql(query, engine)

    # Create Excel file in memory
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        data.to_excel(writer, index=False, sheet_name=table)
        # No need to call save() or close() explicitly; using 'with' handles it
    output.seek(0)

    # Send file
    return send_file(
        output,
        as_attachment=True,
        download_name=f"{table}_data.xlsx",
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )  

@app.route('/8')
def main_dashboard8():
    # Your main code to render data tables
    return render_template('view_data8.html')

def get_db_connection():
    return mysql.connector.connect(**db_config)

@app.route('/download_data8/<table>')
def download_data8(table):
    # Query data from the specified table
    query = f"SELECT * FROM {table}"

    # Get data from the database
    data = pd.read_sql(query, engine)

    # Create Excel file in memory
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        data.to_excel(writer, index=False, sheet_name=table)
        # No need to call save() or close() explicitly; using 'with' handles it
    output.seek(0)

    # Send file
    return send_file(
        output,
        as_attachment=True,
        download_name=f"{table}_data.xlsx",
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )  

@app.route('/9')
def main_dashboard9():
    # Your main code to render data tables
    return render_template('view_data9.html')

def get_db_connection():
    return mysql.connector.connect(**db_config)

@app.route('/download_data9/<table>')
def download_data9(table):
    # Query data from the specified table
    query = f"SELECT * FROM {table}"

    # Get data from the database
    data = pd.read_sql(query, engine)

    # Create Excel file in memory
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        data.to_excel(writer, index=False, sheet_name=table)
        # No need to call save() or close() explicitly; using 'with' handles it
    output.seek(0)

    # Send file
    return send_file(
        output,
        as_attachment=True,
        download_name=f"{table}_data.xlsx",
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )  


@app.route('/10')
def main_dashboard10():
    # Your main code to render data tables
    return render_template('view_data10.html')

def get_db_connection():
    return mysql.connector.connect(**db_config)

@app.route('/download_data10/<table>')
def download_data10(table):
    # Query data from the specified table
    query = f"SELECT * FROM {table}"

    # Get data from the database
    data = pd.read_sql(query, engine)

    # Create Excel file in memory
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        data.to_excel(writer, index=False, sheet_name=table)
        # No need to call save() or close() explicitly; using 'with' handles it
    output.seek(0)

    # Send file
    return send_file(
        output,
        as_attachment=True,
        download_name=f"{table}_data.xlsx",
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )  

# Set a secret key for session management (required for flash messages)
app.secret_key = '040104'  # Replace with a strong, unique key




@app.route("/delete_data", methods=["POST"])
def delete_data():
    try:
        table = request.json.get('table')  # Mengambil nama tabel dari request
        if not table:
            return jsonify({"error": "Nama tabel tidak ditemukan"}), 400
        
        # Membuat query DELETE menggunakan text
        delete_query = text(f"DELETE FROM {table}")  # Gunakan text() untuk query SQL

        with engine.connect() as connection:
            # Eksekusi query DELETE terlebih dahulu
            result = connection.execute(delete_query)

            # Periksa jika tidak ada data yang dihapus
            if result.rowcount == 0:
                return jsonify({"error": "Tidak ada data yang dihapus"}), 400

            # Query ALTER TABLE untuk reset auto increment (Hanya untuk MySQL atau MariaDB)
            try:
                alter_query = text(f"ALTER TABLE {table} AUTO_INCREMENT = 1")
                connection.execute(alter_query)  # Eksekusi ALTER TABLE
            except Exception as alter_error:
                # Jika ALTER gagal, tampilkan pesan bahwa reset auto increment gagal
                print(f"Gagal mereset auto increment: {alter_error}")
                return jsonify({"message": "Data berhasil dihapus, namun gagal mereset auto increment."}), 200

        # Jika berhasil menghapus data dan mereset auto increment, beri pesan sukses
        return jsonify({"message": "Data berhasil dihapus"}), 200

    except Exception as e:
        # Jika ada kesalahan yang terjadi, cetak kesalahan namun kembalikan error spesifik
        print(f"Terjadi kesalahan: {str(e)}")  # Log kesalahan
        return jsonify({"error": "Terjadi kesalahan saat menghapus data."}), 500


@app.route("/delete_data2", methods=["POST"])
def delete_data2():
    try:
        table = request.json.get('table')  # Mengambil nama tabel dari request
        if not table:
            return jsonify({"error": "Nama tabel tidak ditemukan"}), 400
        
        # Membuat query DELETE menggunakan text
        delete_query = text(f"DELETE FROM {table}")  # Gunakan text() untuk query SQL

        with engine.connect() as connection:
            # Eksekusi query DELETE terlebih dahulu
            result = connection.execute(delete_query)

            # Periksa jika tidak ada data yang dihapus
            if result.rowcount == 0:
                return jsonify({"error": "Tidak ada data yang dihapus"}), 400

            # Query ALTER TABLE untuk reset auto increment (Hanya untuk MySQL atau MariaDB)
            try:
                alter_query = text(f"ALTER TABLE {table} AUTO_INCREMENT = 1")
                connection.execute(alter_query)  # Eksekusi ALTER TABLE
            except Exception as alter_error:
                # Jika ALTER gagal, tampilkan pesan bahwa reset auto increment gagal
                print(f"Gagal mereset auto increment: {alter_error}")
                return jsonify({"message": "Data berhasil dihapus, namun gagal mereset auto increment."}), 200

        # Jika berhasil menghapus data dan mereset auto increment, beri pesan sukses
        return jsonify({"message": "Data berhasil dihapus"}), 200

    except Exception as e:
        # Jika ada kesalahan yang terjadi, cetak kesalahan namun kembalikan error spesifik
        print(f"Terjadi kesalahan: {str(e)}")  # Log kesalahan
        return jsonify({"error": "Terjadi kesalahan saat menghapus data."}), 500


@app.route("/delete_data3", methods=["POST"])
def delete_data3():
    try:
        table = request.json.get('table')  # Mengambil nama tabel dari request
        if not table:
            return jsonify({"error": "Nama tabel tidak ditemukan"}), 400
        
        # Membuat query DELETE menggunakan text
        delete_query = text(f"DELETE FROM {table}")  # Gunakan text() untuk query SQL

        with engine.connect() as connection:
            # Eksekusi query DELETE terlebih dahulu
            result = connection.execute(delete_query)

            # Periksa jika tidak ada data yang dihapus
            if result.rowcount == 0:
                return jsonify({"error": "Tidak ada data yang dihapus"}), 400

            # Query ALTER TABLE untuk reset auto increment (Hanya untuk MySQL atau MariaDB)
            try:
                alter_query = text(f"ALTER TABLE {table} AUTO_INCREMENT = 1")
                connection.execute(alter_query)  # Eksekusi ALTER TABLE
            except Exception as alter_error:
                # Jika ALTER gagal, tampilkan pesan bahwa reset auto increment gagal
                print(f"Gagal mereset auto increment: {alter_error}")
                return jsonify({"message": "Data berhasil dihapus, namun gagal mereset auto increment."}), 200

        # Jika berhasil menghapus data dan mereset auto increment, beri pesan sukses
        return jsonify({"message": "Data berhasil dihapus"}), 200

    except Exception as e:
        # Jika ada kesalahan yang terjadi, cetak kesalahan namun kembalikan error spesifik
        print(f"Terjadi kesalahan: {str(e)}")  # Log kesalahan
        return jsonify({"error": "Terjadi kesalahan saat menghapus data."}), 500


@app.route("/delete_data4", methods=["POST"])
def delete_data4():
    try:
        table = request.json.get('table')  # Mengambil nama tabel dari request
        if not table:
            return jsonify({"error": "Nama tabel tidak ditemukan"}), 400
        
        # Membuat query DELETE menggunakan text
        delete_query = text(f"DELETE FROM {table}")  # Gunakan text() untuk query SQL

        with engine.connect() as connection:
            # Eksekusi query DELETE terlebih dahulu
            result = connection.execute(delete_query)

            # Periksa jika tidak ada data yang dihapus
            if result.rowcount == 0:
                return jsonify({"error": "Tidak ada data yang dihapus"}), 400

            # Query ALTER TABLE untuk reset auto increment (Hanya untuk MySQL atau MariaDB)
            try:
                alter_query = text(f"ALTER TABLE {table} AUTO_INCREMENT = 1")
                connection.execute(alter_query)  # Eksekusi ALTER TABLE
            except Exception as alter_error:
                # Jika ALTER gagal, tampilkan pesan bahwa reset auto increment gagal
                print(f"Gagal mereset auto increment: {alter_error}")
                return jsonify({"message": "Data berhasil dihapus, namun gagal mereset auto increment."}), 200

        # Jika berhasil menghapus data dan mereset auto increment, beri pesan sukses
        return jsonify({"message": "Data berhasil dihapus"}), 200

    except Exception as e:
        # Jika ada kesalahan yang terjadi, cetak kesalahan namun kembalikan error spesifik
        print(f"Terjadi kesalahan: {str(e)}")  # Log kesalahan
        return jsonify({"error": "Terjadi kesalahan saat menghapus data."}), 500

@app.route("/delete_data5", methods=["POST"])
def delete_data5():
    try:
        table = request.json.get('table')  # Mengambil nama tabel dari request
        if not table:
            return jsonify({"error": "Nama tabel tidak ditemukan"}), 400
        
        # Membuat query DELETE menggunakan text
        delete_query = text(f"DELETE FROM {table}")  # Gunakan text() untuk query SQL

        with engine.connect() as connection:
            # Eksekusi query DELETE terlebih dahulu
            result = connection.execute(delete_query)

            # Periksa jika tidak ada data yang dihapus
            if result.rowcount == 0:
                return jsonify({"error": "Tidak ada data yang dihapus"}), 400

            # Query ALTER TABLE untuk reset auto increment (Hanya untuk MySQL atau MariaDB)
            try:
                alter_query = text(f"ALTER TABLE {table} AUTO_INCREMENT = 1")
                connection.execute(alter_query)  # Eksekusi ALTER TABLE
            except Exception as alter_error:
                # Jika ALTER gagal, tampilkan pesan bahwa reset auto increment gagal
                print(f"Gagal mereset auto increment: {alter_error}")
                return jsonify({"message": "Data berhasil dihapus, namun gagal mereset auto increment."}), 200

        # Jika berhasil menghapus data dan mereset auto increment, beri pesan sukses
        return jsonify({"message": "Data berhasil dihapus"}), 200

    except Exception as e:
        # Jika ada kesalahan yang terjadi, cetak kesalahan namun kembalikan error spesifik
        print(f"Terjadi kesalahan: {str(e)}")  # Log kesalahan
        return jsonify({"error": "Terjadi kesalahan saat menghapus data."}), 500


@app.route("/delete_data6", methods=["POST"])
def delete_data6():
    try:
        table = request.json.get('table')  # Mengambil nama tabel dari request
        if not table:
            return jsonify({"error": "Nama tabel tidak ditemukan"}), 400
        
        # Membuat query DELETE menggunakan text
        delete_query = text(f"DELETE FROM {table}")  # Gunakan text() untuk query SQL

        with engine.connect() as connection:
            # Eksekusi query DELETE terlebih dahulu
            result = connection.execute(delete_query)

            # Periksa jika tidak ada data yang dihapus
            if result.rowcount == 0:
                return jsonify({"error": "Tidak ada data yang dihapus"}), 400

            # Query ALTER TABLE untuk reset auto increment (Hanya untuk MySQL atau MariaDB)
            try:
                alter_query = text(f"ALTER TABLE {table} AUTO_INCREMENT = 1")
                connection.execute(alter_query)  # Eksekusi ALTER TABLE
            except Exception as alter_error:
                # Jika ALTER gagal, tampilkan pesan bahwa reset auto increment gagal
                print(f"Gagal mereset auto increment: {alter_error}")
                return jsonify({"message": "Data berhasil dihapus, namun gagal mereset auto increment."}), 200

        # Jika berhasil menghapus data dan mereset auto increment, beri pesan sukses
        return jsonify({"message": "Data berhasil dihapus"}), 200

    except Exception as e:
        # Jika ada kesalahan yang terjadi, cetak kesalahan namun kembalikan error spesifik
        print(f"Terjadi kesalahan: {str(e)}")  # Log kesalahan
        return jsonify({"error": "Terjadi kesalahan saat menghapus data."}), 500

@app.route("/delete_data7", methods=["POST"])
def delete_data7():
    try:
        table = request.json.get('table')  # Mengambil nama tabel dari request
        if not table:
            return jsonify({"error": "Nama tabel tidak ditemukan"}), 400
        
        # Membuat query DELETE menggunakan text
        delete_query = text(f"DELETE FROM {table}")  # Gunakan text() untuk query SQL

        with engine.connect() as connection:
            # Eksekusi query DELETE terlebih dahulu
            result = connection.execute(delete_query)

            # Periksa jika tidak ada data yang dihapus
            if result.rowcount == 0:
                return jsonify({"error": "Tidak ada data yang dihapus"}), 400

            # Query ALTER TABLE untuk reset auto increment (Hanya untuk MySQL atau MariaDB)
            try:
                alter_query = text(f"ALTER TABLE {table} AUTO_INCREMENT = 1")
                connection.execute(alter_query)  # Eksekusi ALTER TABLE
            except Exception as alter_error:
                # Jika ALTER gagal, tampilkan pesan bahwa reset auto increment gagal
                print(f"Gagal mereset auto increment: {alter_error}")
                return jsonify({"message": "Data berhasil dihapus, namun gagal mereset auto increment."}), 200

        # Jika berhasil menghapus data dan mereset auto increment, beri pesan sukses
        return jsonify({"message": "Data berhasil dihapus"}), 200

    except Exception as e:
        # Jika ada kesalahan yang terjadi, cetak kesalahan namun kembalikan error spesifik
        print(f"Terjadi kesalahan: {str(e)}")  # Log kesalahan
        return jsonify({"error": "Terjadi kesalahan saat menghapus data."}), 500

@app.route("/delete_data8", methods=["POST"])
def delete_data8():
    try:
        table = request.json.get('table')  # Mengambil nama tabel dari request
        if not table:
            return jsonify({"error": "Nama tabel tidak ditemukan"}), 400
        
        # Membuat query DELETE menggunakan text
        delete_query = text(f"DELETE FROM {table}")  # Gunakan text() untuk query SQL

        with engine.connect() as connection:
            # Eksekusi query DELETE terlebih dahulu
            result = connection.execute(delete_query)

            # Periksa jika tidak ada data yang dihapus
            if result.rowcount == 0:
                return jsonify({"error": "Tidak ada data yang dihapus"}), 400

            # Query ALTER TABLE untuk reset auto increment (Hanya untuk MySQL atau MariaDB)
            try:
                alter_query = text(f"ALTER TABLE {table} AUTO_INCREMENT = 1")
                connection.execute(alter_query)  # Eksekusi ALTER TABLE
            except Exception as alter_error:
                # Jika ALTER gagal, tampilkan pesan bahwa reset auto increment gagal
                print(f"Gagal mereset auto increment: {alter_error}")
                return jsonify({"message": "Data berhasil dihapus, namun gagal mereset auto increment."}), 200

        # Jika berhasil menghapus data dan mereset auto increment, beri pesan sukses
        return jsonify({"message": "Data berhasil dihapus"}), 200

    except Exception as e:
        # Jika ada kesalahan yang terjadi, cetak kesalahan namun kembalikan error spesifik
        print(f"Terjadi kesalahan: {str(e)}")  # Log kesalahan
        return jsonify({"error": "Terjadi kesalahan saat menghapus data."}), 500

@app.route("/delete_data9", methods=["POST"])
def delete_data9():
    try:
        table = request.json.get('table')  # Mengambil nama tabel dari request
        if not table:
            return jsonify({"error": "Nama tabel tidak ditemukan"}), 400
        
        # Membuat query DELETE menggunakan text
        delete_query = text(f"DELETE FROM {table}")  # Gunakan text() untuk query SQL

        with engine.connect() as connection:
            # Eksekusi query DELETE terlebih dahulu
            result = connection.execute(delete_query)

            # Periksa jika tidak ada data yang dihapus
            if result.rowcount == 0:
                return jsonify({"error": "Tidak ada data yang dihapus"}), 400

            # Query ALTER TABLE untuk reset auto increment (Hanya untuk MySQL atau MariaDB)
            try:
                alter_query = text(f"ALTER TABLE {table} AUTO_INCREMENT = 1")
                connection.execute(alter_query)  # Eksekusi ALTER TABLE
            except Exception as alter_error:
                # Jika ALTER gagal, tampilkan pesan bahwa reset auto increment gagal
                print(f"Gagal mereset auto increment: {alter_error}")
                return jsonify({"message": "Data berhasil dihapus, namun gagal mereset auto increment."}), 200

        # Jika berhasil menghapus data dan mereset auto increment, beri pesan sukses
        return jsonify({"message": "Data berhasil dihapus"}), 200

    except Exception as e:
        # Jika ada kesalahan yang terjadi, cetak kesalahan namun kembalikan error spesifik
        print(f"Terjadi kesalahan: {str(e)}")  # Log kesalahan
        return jsonify({"error": "Terjadi kesalahan saat menghapus data."}), 500


@app.route("/delete_data10", methods=["POST"])
def delete_data10():
    try:
        table = request.json.get('table')  # Mengambil nama tabel dari request
        if not table:
            return jsonify({"error": "Nama tabel tidak ditemukan"}), 400
        
        # Membuat query DELETE menggunakan text
        delete_query = text(f"DELETE FROM {table}")  # Gunakan text() untuk query SQL

        with engine.connect() as connection:
            # Eksekusi query DELETE terlebih dahulu
            result = connection.execute(delete_query)

            # Periksa jika tidak ada data yang dihapus
            if result.rowcount == 0:
                return jsonify({"error": "Tidak ada data yang dihapus"}), 400

            # Query ALTER TABLE untuk reset auto increment (Hanya untuk MySQL atau MariaDB)
            try:
                alter_query = text(f"ALTER TABLE {table} AUTO_INCREMENT = 1")
                connection.execute(alter_query)  # Eksekusi ALTER TABLE
            except Exception as alter_error:
                # Jika ALTER gagal, tampilkan pesan bahwa reset auto increment gagal
                print(f"Gagal mereset auto increment: {alter_error}")
                return jsonify({"message": "Data berhasil dihapus, namun gagal mereset auto increment."}), 200

        # Jika berhasil menghapus data dan mereset auto increment, beri pesan sukses
        return jsonify({"message": "Data berhasil dihapus"}), 200

    except Exception as e:
        # Jika ada kesalahan yang terjadi, cetak kesalahan namun kembalikan error spesifik
        print(f"Terjadi kesalahan: {str(e)}")  # Log kesalahan
        return jsonify({"error": "Terjadi kesalahan saat menghapus data."}), 500

@app.route('/update_data/<table>', methods=['POST'])
def update_data(table):
    try:
        updated_data = request.form.to_dict()  # Retrieve and prepare updated data from the form

        # Logic to update data based on the table parameter
        # if table == 'billing_result':
        #     pass  # Update logic for billing_result
        if table == 'billing_naik':
            pass  # Update logic for billing_naik
        elif table == 'billing_turun':
            pass  # Update logic for billing_turun
        # elif table == 'billing_aman':
        #     pass  # Update logic for billing_aman
        elif table == 'billing_div':
            pass  # Update logic for billing_aman
        else:
            flash("Invalid table name.")
            return redirect(url_for('view_data'))

        flash("Data updated successfully!")
    except Exception as e:
        flash(f"Error updating data: {str(e)}")
    
    return redirect(url_for('view_data'))

@app.route('/update_data2/<table>', methods=['POST'])
def update_data2(table):
    try:
        updated_data = request.form.to_dict()  # Retrieve and prepare updated data from the form

        # Logic to update data based on the table parameter
        # if table == 'billing_result':
        #     pass  # Update logic for billing_result
        if table == 'billing_naik2':
            pass  # Update logic for billing_naik
        elif table == 'billing_turun2':
            pass  # Update logic for billing_turun
        # elif table == 'billing_aman':
        #     pass  # Update logic for billing_aman
        elif table == 'billing_div2':
            pass  # Update logic for billing_aman
        else:                                                                           
            flash("Invalid table name.")
            return redirect(url_for('view_data2'))

        flash("Data updated successfully!")
    except Exception as e:
        flash(f"Error updating data: {str(e)}")
    
    return redirect(url_for('view_data2'))

@app.route('/update_data3/<table>', methods=['POST'])
def update_data3(table):
    try:
        updated_data = request.form.to_dict()  # Retrieve and prepare updated data from the form

        # Logic to update data based on the table parameter
        # if table == 'billing_result':
        #     pass  # Update logic for billing_result
        if table == 'billing_naik3':
            pass  # Update logic for billing_naik
        elif table == 'billing_turun3':
            pass  # Update logic for billing_turun
        # elif table == 'billing_aman':
        #     pass  # Update logic for billing_aman
        elif table == 'billing_div3':
            pass  # Update logic for billing_aman
        else:                                                                           
            flash("Invalid table name.")
            return redirect(url_for('view_data3'))

        flash("Data updated successfully!")
    except Exception as e:
        flash(f"Error updating data: {str(e)}")
    
    return redirect(url_for('view_data3'))


@app.route('/update_data4/<table>', methods=['POST'])
def update_data4(table):
    try:
        updated_data = request.form.to_dict()  # Retrieve and prepare updated data from the form

        # Logic to update data based on the table parameter
        # if table == 'billing_result':
        #     pass  # Update logic for billing_result
        if table == 'billing_naik4':
            pass  # Update logic for billing_naik
        elif table == 'billing_turun4':
            pass  # Update logic for billing_turun
        # elif table == 'billing_aman':
        #     pass  # Update logic for billing_aman
        elif table == 'billing_div4':
            pass  # Update logic for billing_aman
        else:                                                                           
            flash("Invalid table name.")
            return redirect(url_for('view_data4'))

        flash("Data updated successfully!")
    except Exception as e:
        flash(f"Error updating data: {str(e)}")
    
    return redirect(url_for('view_data4'))


@app.route('/update_data5/<table>', methods=['POST'])
def update_data5(table):
    try:
        updated_data = request.form.to_dict()  # Retrieve and prepare updated data from the form

        # Logic to update data based on the table parameter
        # if table == 'billing_result':
        #     pass  # Update logic for billing_result
        if table == 'billing_naik5':
            pass  # Update logic for billing_naik
        elif table == 'billing_turun5':
            pass  # Update logic for billing_turun
        # elif table == 'billing_aman':
        #     pass  # Update logic for billing_aman
        elif table == 'billing_div5':
            pass  # Update logic for billing_aman
        else:                                                                           
            flash("Invalid table name.")
            return redirect(url_for('view_data5'))

        flash("Data updated successfully!")
    except Exception as e:
        flash(f"Error updating data: {str(e)}")
    
    return redirect(url_for('view_data5'))


@app.route('/update_data6/<table>', methods=['POST'])
def update_data6(table):
    try:
        updated_data = request.form.to_dict()  # Retrieve and prepare updated data from the form

        # Logic to update data based on the table parameter
        # if table == 'billing_result':
        #     pass  # Update logic for billing_result
        if table == 'billing_naik6':
            pass  # Update logic for billing_naik
        elif table == 'billing_turun6':
            pass  # Update logic for billing_turun
        # elif table == 'billing_aman':
        #     pass  # Update logic for billing_aman
        elif table == 'billing_div6':
            pass  # Update logic for billing_aman
        else:                                                                           
            flash("Invalid table name.")
            return redirect(url_for('view_data6'))

        flash("Data updated successfully!")
    except Exception as e:
        flash(f"Error updating data: {str(e)}")
    
    return redirect(url_for('view_data6'))

@app.route('/update_data7/<table>', methods=['POST'])
def update_data7(table):
    try:
        updated_data = request.form.to_dict()  # Retrieve and prepare updated data from the form

        # Logic to update data based on the table parameter
        # if table == 'billing_result':
        #     pass  # Update logic for billing_result
        if table == 'billing_naik7':
            pass  # Update logic for billing_naik
        elif table == 'billing_turun7':
            pass  # Update logic for billing_turun
        # elif table == 'billing_aman':
        #     pass  # Update logic for billing_aman
        elif table == 'billing_div7':
            pass  # Update logic for billing_aman
        else:                                                                           
            flash("Invalid table name.")
            return redirect(url_for('view_data7'))

        flash("Data updated successfully!")
    except Exception as e:
        flash(f"Error updating data: {str(e)}")
    
    return redirect(url_for('view_data7'))

@app.route('/update_data8/<table>', methods=['POST'])
def update_data8(table):
    try:
        updated_data = request.form.to_dict()  # Retrieve and prepare updated data from the form

        # Logic to update data based on the table parameter
        # if table == 'billing_result':
        #     pass  # Update logic for billing_result
        if table == 'billing_naik8':
            pass  # Update logic for billing_naik
        elif table == 'billing_turun8':
            pass  # Update logic for billing_turun
        # elif table == 'billing_aman':
        #     pass  # Update logic for billing_aman
        elif table == 'billing_div8':
            pass  # Update logic for billing_aman
        else:                                                                           
            flash("Invalid table name.")
            return redirect(url_for('view_data8'))

        flash("Data updated successfully!")
    except Exception as e:
        flash(f"Error updating data: {str(e)}")
    
    return redirect(url_for('view_data8'))

@app.route('/update_data9/<table>', methods=['POST'])
def update_data9(table):
    try:
        updated_data = request.form.to_dict()  # Retrieve and prepare updated data from the form

        # Logic to update data based on the table parameter
        # if table == 'billing_result':
        #     pass  # Update logic for billing_result
        if table == 'billing_naik9':
            pass  # Update logic for billing_naik
        elif table == 'billing_turun9':
            pass  # Update logic for billing_turun
        # elif table == 'billing_aman':
        #     pass  # Update logic for billing_aman
        elif table == 'billing_div9':
            pass  # Update logic for billing_aman
        else:                                                                           
            flash("Invalid table name.")
            return redirect(url_for('view_data9'))

        flash("Data updated successfully!")
    except Exception as e:
        flash(f"Error updating data: {str(e)}")
    
    return redirect(url_for('view_data9'))


@app.route('/update_data10/<table>', methods=['POST'])
def update_data10(table):
    try:
        updated_data = request.form.to_dict()  # Retrieve and prepare updated data from the form

        # Logic to update data based on the table parameter
        # if table == 'billing_result':
        #     pass  # Update logic for billing_result
        if table == 'billing_naik10':
            pass  # Update logic for billing_naik
        elif table == 'billing_turun10':
            pass  # Update logic for billing_turun
        # elif table == 'billing_aman':
        #     pass  # Update logic for billing_aman
        elif table == 'billing_div10':
            pass  # Update logic for billing_aman
        else:                                                                           
            flash("Invalid table name.")
            return redirect(url_for('view_data10'))

        flash("Data updated successfully!")
    except Exception as e:
        flash(f"Error updating data: {str(e)}")
    
    return redirect(url_for('view_data10'))

if __name__ == '__main__':
    app.run(debug=True) 
