from flask import Flask, render_template, request, redirect, url_for, session, flash
from flask_mysqldb import MySQL
import MySQLdb.cursors
import hashlib
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
import os
from werkzeug.utils import secure_filename
from sqlalchemy import BigInteger, String, Float, Text
from flask import render_template, request, redirect, url_for, flash, session
import pandas as pd
import numpy as np
from sqlalchemy import text, String, BigInteger


# =================== APP ===================
app = Flask(__name__)
app.secret_key = 'your_secret_key'

# =================== MYSQL CONFIG ===================
app.config['MYSQL_HOST'] = 'localhost'
app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = ''
app.config['MYSQL_DB'] = 'berkah_billing'

mysql = MySQL(app)
engine = create_engine("mysql+pymysql://root:@localhost/berkah_billing")

# =================== UPLOAD CONFIG ===================
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'xlsx', 'xls'}
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# =================== HELPER ===================
def normalize_blth(blth_str):
    if blth_str is None:
        return datetime.now().strftime('%Y%m')
    return blth_str.replace('-', '').replace('/', '')

def get_previous_blth(blth_str, months_back=1):
    date = datetime.strptime(blth_str,'%Y%m')
    prev_date = date - relativedelta(months=months_back)
    return prev_date.strftime('%Y%m')

# =================== LOGIN ===================
@app.route('/', methods=['GET','POST'])
def login():
    if request.method=='POST':
        username = request.form['username']
        password_input = request.form['password']

        cursor = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute("SELECT * FROM tb_user WHERE username=%s", [username])
        user = cursor.fetchone()

        if user:
            hashed_input = hashlib.sha256(password_input.encode()).hexdigest()
            if hashed_input == user['password']:
                session['loggedin'] = True
                session['username'] = user['username']
                session['nama_ulp'] = user['nama_ulp']
                session['role'] = user.get('role','ULP')
                session['unitup'] = user.get('unitup',None)
                flash('Login berhasil','success')
                if session['role'].upper()=='UP3':
                    return redirect(url_for('dashboard_up3'))
                else:
                    return redirect(url_for('dashboard_ulp'))
            else:
                flash('Password salah!','danger')
        else:
            flash('Username tidak ditemukan!','danger')
    return render_template('login.html')

@app.route('/kelola_user')
def kelola_user():
    if 'loggedin' not in session:
        return redirect(url_for('login'))
    cursor = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("SELECT id_user, username, nama_ulp, unitup, role FROM tb_user")
    users = cursor.fetchall()
    return render_template('kelola_user.html', users=users)

@app.route('/tambah_user', methods=['POST'])
def tambah_user():
    if 'loggedin' not in session or session.get('role') != 'UP3':
        return redirect(url_for('login'))

    unitup = request.form['unitup']
    nama_ulp = request.form['nama_ulp']
    username = request.form['username']
    password = request.form['password']

    hashed_pw = hashlib.sha256(password.encode()).hexdigest()

    cursor = mysql.connection.cursor()
    cursor.execute("""
        INSERT INTO tb_user (unitup, nama_ulp, username, password, role)
        VALUES (%s, %s, %s, %s, 'ULP')
    """, (unitup, nama_ulp, username, hashed_pw))
    mysql.connection.commit()

    flash('User berhasil ditambahkan!', 'success')
    return redirect(url_for('kelola_user'))

@app.route('/hapus_user/<int:id_user>')
def hapus_user(id_user):
    if 'loggedin' not in session or session.get('role') != 'UP3':
        return redirect(url_for('login'))

    cursor = mysql.connection.cursor()
    cursor.execute("DELETE FROM tb_user WHERE id_user = %s", [id_user])
    mysql.connection.commit()
    flash('User berhasil dihapus!', 'success')
    return redirect(url_for('kelola_user'))

@app.route('/logout')
def logout():
    session.clear()
    flash('Anda telah logout', 'success')
    return redirect(url_for('login'))

# =================== DASHBOARD ===================
@app.route('/dashboard_ulp')
def dashboard_ulp():
    if 'loggedin' not in session:
        return redirect(url_for('login'))
    return render_template('dashboard_ulp.html',
                           nama=session['nama_ulp'],
                           unitup=session.get('unitup','-'))

@app.route('/dashboard_up3')
def dashboard_up3():
    if 'loggedin' not in session:
        return redirect(url_for('login'))
    return render_template('dashboard_up3.html', nama=session['nama_ulp'])

# =================== DASHBOARD RUNNING BILLING ===================
# @app.route('/dashboard_running_billing/', methods=['GET','POST'])
# def dashboard_running_billing():
#     if 'loggedin' not in session:
#         return redirect(url_for('login'))

#     blth_kini = normalize_blth(request.form.get('blth', datetime.now().strftime('%Y%m')))
#     blth_lalu = get_previous_blth(blth_kini, 1)
#     blth_lalulalu = get_previous_blth(blth_kini, 2)

#     # ===== UPLOAD DPM =====
#     if request.method=='POST':
#         file = request.files.get('file')
#         if not file or file.filename=='':
#             flash('Tidak ada file yang dipilih', 'danger')
#             return redirect(url_for('dashboard_running_billing'))

#         if not allowed_file(file.filename):
#             flash('Format file tidak didukung (hanya .xlsx atau .xls)', 'danger')
#             return redirect(url_for('dashboard_running_billing'))

#         filename = secure_filename(file.filename)
#         filepath = os.path.join(UPLOAD_FOLDER, filename)
#         file.save(filepath)

#         try:
#             df_upload = pd.read_excel(filepath)
#             df_upload.columns = [c.strip().upper() for c in df_upload.columns]

#             # ===== Kolom yang ada di database =====
#             db_cols = ['BLTH','IDPEL','NAMA','TARIF','DAYA','SLALWBP','LWBPCABUT',
#                        'LWBPPASANG','SAHLWBP','LWBPPAKAI','DLPD']
#             df_upload = df_upload[[c for c in df_upload.columns if c in db_cols]]

#             # Tambahkan BLTH jika tidak ada
#             if 'BLTH' not in df_upload.columns:
#                 df_upload['BLTH'] = blth_kini
#             else:
#                 df_upload['BLTH'] = blth_kini

#             # Pastikan kolom numerik
#             numeric_cols = ['DAYA','SLALWBP','LWBPCABUT','LWBPPASANG','SAHLWBP','LWBPPAKAI']
#             for col in numeric_cols:
#                 if col in df_upload.columns:
#                     df_upload[col] = pd.to_numeric(df_upload[col], errors='coerce').fillna(0)

#             if 'DLPD' in df_upload.columns:
#                 df_upload['DLPD'] = df_upload['DLPD'].astype(str).fillna('')

#             # Simpan ke DPM
#             df_upload.to_sql('dpm', engine, if_exists='append', index=False)
#             flash(f'File {filename} berhasil diupload dan disimpan ke database.', 'success')
#         except Exception as e:
#             flash(f'Gagal memproses file: {e}', 'danger')
#             return redirect(url_for('dashboard_running_billing'))

#     # ===== AMBIL DATA 3 BULAN TERAKHIR =====
#     try:
#         query = text(f"""
#             SELECT * FROM dpm 
#             WHERE BLTH IN ('{blth_kini}','{blth_lalu}','{blth_lalulalu}')
#         """)
#         df = pd.read_sql(query, engine)
#     except Exception as e:
#         flash(f"Gagal membaca data DPM: {e}", 'danger')
#         return render_template('dashboard_running_billing.html', naik=[], turun=[], div=[], blth_terakhir=blth_kini)

#     if df.empty:
#         flash("Belum ada data DPM untuk periode ini.", "info")
#         return render_template('dashboard_running_billing.html', naik=[], turun=[], div=[], blth_terakhir=blth_kini)

#     # ===== HITUNG DELTA & PERSEN =====
#     df.columns = [c.upper() for c in df.columns]
#     df_kini = df[df['BLTH']==blth_kini]
#     df_lalu = df[df['BLTH']==blth_lalu]

#     merged = df_kini.merge(df_lalu[['IDPEL','SAHLWBP']], on='IDPEL', how='left', suffixes=('_KINI','_LALU'))
#     merged['SAHLWBP_LALU'] = merged['SAHLWBP_LALU'].fillna(0)
#     merged['DELTA_KWH'] = merged['SAHLWBP_KINI'] - merged['SAHLWBP_LALU']
#     merged['PERSEN'] = np.where(merged['SAHLWBP_LALU']>0, (merged['DELTA_KWH']/merged['SAHLWBP_LALU'])*100, 0)

#     naik_df = merged[merged['PERSEN']>=40]
#     turun_df = merged[merged['PERSEN']<=-40]
#     div_df = merged[(merged['SAHLWBP_KINI'].isna()) | (merged['SAHLWBP_LALU'].isna())]

#     # ===== SIMPAN KE BILLING =====
#     try:
#         merged.to_sql('billing', engine, if_exists='replace', index=False)
#         naik_df.to_sql('billing_naik', engine, if_exists='replace', index=False)
#         turun_df.to_sql('billing_turun', engine, if_exists='replace', index=False)
#         div_df.to_sql('billing_div', engine, if_exists='replace', index=False)
#     except Exception as e:
#         flash(f"Gagal menyimpan hasil billing ke database: {e}", 'warning')

#     return render_template(
#         'dashboard_running_billing.html',
#         naik=naik_df.to_dict(orient='records'),
#         turun=turun_df.to_dict(orient='records'),
#         div=div_df.to_dict(orient='records'),
#         blth_terakhir=blth_kini
#     )

########################################################



@app.route('/dashboard_running_billing', methods=['GET', 'POST'])
def dashboard_running_billing():
    if 'loggedin' not in session:
        return redirect(url_for('login'))

    blth_kini = normalize_blth(request.form.get('blth', datetime.now().strftime('%Y%m')))
    blth_lalu = get_previous_blth(blth_kini, 1)
    blth_lalulalu = get_previous_blth(blth_kini, 2)

    # ===== Upload File DPM =====
    if request.method == 'POST':
        file = request.files.get('file')
        if not file or file.filename == '':
            flash('Tidak ada file yang dipilih', 'danger')
            return redirect(url_for('dashboard_running_billing'))

        if not allowed_file(file.filename):
            flash('Format file tidak didukung (hanya .xlsx/.xls)', 'danger')
            return redirect(url_for('dashboard_running_billing'))

        filename = secure_filename(file.filename)
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        file.save(filepath)

        try:
            df_upload = pd.read_excel(filepath)
            df_upload.columns = [c.strip().upper() for c in df_upload.columns]

            db_cols = ['BLTH', 'IDPEL', 'NAMA', 'TARIF', 'DAYA', 'SLALWBP',
                       'LWBPCABUT', 'LWBPPASANG', 'SAHLWBP', 'LWBPPAKAI', 'DLPD']
            df_upload = df_upload[[c for c in df_upload.columns if c in db_cols]]

            df_upload['BLTH'] = blth_kini

            # Pastikan kolom numerik valid
            numeric_cols = ['DAYA', 'SLALWBP', 'LWBPCABUT', 'LWBPPASANG', 'SAHLWBP', 'LWBPPAKAI']
            for col in numeric_cols:
                if col in df_upload.columns:
                    df_upload[col] = pd.to_numeric(df_upload[col], errors='coerce').fillna(0)

            if 'DLPD' in df_upload.columns:
                df_upload['DLPD'] = df_upload['DLPD'].astype(str).fillna('')

            df_upload.to_sql('dpm', engine, if_exists='append', index=False)
            flash(f'File {filename} berhasil diupload ke database.', 'success')
        except Exception as e:
            flash(f'Gagal memproses file DPM: {e}', 'danger')
            return redirect(url_for('dashboard_running_billing'))

    # ===== Ambil Data 3 Bulan =====
    try:
        query = text(f"""
            SELECT * FROM dpm
            WHERE BLTH IN ('{blth_kini}', '{blth_lalu}', '{blth_lalulalu}')
        """)
        df = pd.read_sql(query, engine)
    except Exception as e:
        flash(f"Gagal membaca data DPM: {e}", 'danger')
        return render_template('dashboard_running_billing.html', naik=[], turun=[], div=[], blth_terakhir=blth_kini)

    if df.empty:
        flash("Belum ada data DPM untuk periode ini.", "info")
        return render_template('dashboard_running_billing.html', naik=[], turun=[], div=[], blth_terakhir=blth_kini)

    # ===== Filter per bulan =====
    df.columns = [c.upper() for c in df.columns]
    df_kini = df[df['BLTH'] == blth_kini]
    df_lalu = df[df['BLTH'] == blth_lalu]
    df_lalulalu = df[df['BLTH'] == blth_lalulalu]

    # ===== Proses Billing =====
    def copy_dataframe(lalulalu, lalu, kini):
        juruslalulalu = lalulalu[['IDPEL', 'LWBPPAKAI']].copy()
        juruslalu = lalu[['IDPEL', 'LWBPPAKAI']].copy()
        juruskini = kini.copy()

        kroscek_temp = pd.merge(pd.merge(juruslalulalu, juruslalu, on='IDPEL', how='right'),
                                juruskini, on='IDPEL', how='right')

        delta = kroscek_temp['LWBPPAKAI'] - kroscek_temp['LWBPPAKAI_y']
        percentage = (delta / kroscek_temp['LWBPPAKAI_y'].replace(0, np.nan)) * 100
        percentage = np.nan_to_num(percentage, nan=0, posinf=0, neginf=0)

        kroscek = pd.DataFrame({
            'BLTH': blth_kini,
            'IDPEL': kroscek_temp['IDPEL'],
            'NAMA': kroscek_temp.get('NAMA', ''),
            'TARIF': kroscek_temp.get('TARIF', ''),
            'DAYA': pd.to_numeric(kroscek_temp['DAYA'], errors='coerce').fillna(0).astype(int),
            'SLALWBP': pd.to_numeric(kroscek_temp['SLALWBP'], errors='coerce').fillna(0).astype(int),
            'LWBPCABUT': pd.to_numeric(kroscek_temp['LWBPCABUT'], errors='coerce').fillna(0).astype(int),
            'SELISIH_STAN_BONGKAR': (kroscek_temp['SLALWBP'] - kroscek_temp['LWBPCABUT']).fillna(0).astype(int),
            'LWBPPASANG': pd.to_numeric(kroscek_temp['LWBPPASANG'], errors='coerce').fillna(0).astype(int),
            'SAHLWBP': pd.to_numeric(kroscek_temp['SAHLWBP'], errors='coerce').fillna(0).astype(int),
            'KWH_SEKARANG': kroscek_temp['LWBPPAKAI'].fillna(0).astype(int),
            'KWH_1_BULAN_LALU': kroscek_temp['LWBPPAKAI_y'].fillna(0).astype(int),
            'KWH_2_BULAN_LALU': kroscek_temp['LWBPPAKAI_x'].fillna(0).astype(int),
            'DELTA_PEMKWH': delta.fillna(0).astype(int),
            'PERSEN': (percentage.round(0)).astype(int).astype(str) + '%',
            'KET': np.where(
                kroscek_temp['LWBPPAKAI_y'].isna() | (kroscek_temp['LWBPPAKAI_y'] == 0),
                'DIV/NA',
                np.where(percentage >= 40, 'NAIK',
                         np.where(percentage <= -40, 'TURUN', 'AMAN'))
            ),
            'DLPD': kroscek_temp.get('DLPD', '')
        })

        # ===== Tambahkan kolom FOTO + dropdown =====
        path_foto1 = 'https://portalapp.iconpln.co.id/acmt/DisplayBlobServlet1?idpel='
        path_foto2 = '&blth='
        kroscek['FOTO AKHIR'] = kroscek['IDPEL'].apply(lambda x: f'{path_foto1}{x}{path_foto2}{blth_kini}')
        kroscek['FOTO LALU'] = kroscek['IDPEL'].apply(lambda x: f'{path_foto1}{x}{path_foto2}{blth_lalu}')
        kroscek['FOTO LALU2'] = kroscek['IDPEL'].apply(lambda x: f'{path_foto1}{x}{path_foto2}{blth_lalulalu}')

        kroscek['HASIL PEMERIKSAAN'] = ''
        kroscek['TINDAK LANJUT'] = ''
        kroscek['KETERANGAN'] = ''

        return kroscek

    # Jalankan fungsi
    kroscek = copy_dataframe(df_lalulalu, df_lalu, df_kini)

    # ===== Simpan ke database =====
    dtype_billing = {
        'BLTH': String(20),
        'IDPEL': String(30),
        'NAMA': String(100),
        'TARIF': String(20),
        'DAYA': BigInteger(),
        'SLALWBP': BigInteger(),
        'LWBPCABUT': BigInteger(),
        'SELISIH_STAN_BONGKAR': BigInteger(),
        'LWBPPASANG': BigInteger(),
        'SAHLWBP': BigInteger(),
        'KWH_SEKARANG': BigInteger(),
        'KWH_1_BULAN_LALU': BigInteger(),
        'KWH_2_BULAN_LALU': BigInteger(),
        'DELTA_PEMKWH': BigInteger(),
        'PERSEN': String(10),
        'KET': String(20),
        'DLPD': String(100),
        'FOTO AKHIR': String(255),
        'FOTO LALU': String(255),
        'FOTO LALU2': String(255),
        'HASIL PEMERIKSAAN': String(50),
        'TINDAK LANJUT': Text(),
        'KETERANGAN': String(100)
    }

    try:
        kroscek.to_sql('billing', engine, if_exists='replace', index=False, dtype=dtype_billing)
        kroscek[kroscek['KET'] == 'NAIK'].to_sql('billing_naik', engine, if_exists='replace', index=False, dtype=dtype_billing)
        kroscek[kroscek['KET'] == 'TURUN'].to_sql('billing_turun', engine, if_exists='replace', index=False, dtype=dtype_billing)
        kroscek[kroscek['KET'] == 'DIV/NA'].to_sql('billing_div', engine, if_exists='replace', index=False, dtype=dtype_billing)
        flash('Perhitungan billing berhasil disimpan ke database.', 'success')
    except Exception as e:
        flash(f'Gagal menyimpan hasil billing: {e}', 'danger')

    return render_template(
        'dashboard_running_billing.html',
        naik=kroscek[kroscek['KET'] == 'NAIK'].to_dict(orient='records'),
        turun=kroscek[kroscek['KET'] == 'TURUN'].to_dict(orient='records'),
        div=kroscek[kroscek['KET'] == 'DIV/NA'].to_dict(orient='records'),
        blth_terakhir=blth_kini
    )






# =================== VIEW DATA ===================
@app.route("/view_data")
def view_data():
    data_naik = pd.read_sql("SELECT * FROM billing_naik", engine)
    data_turun = pd.read_sql("SELECT * FROM billing_turun", engine)
    data_div = pd.read_sql("SELECT * FROM billing_div", engine)

    return render_template(
        "view_data.html",
        naik_html=data_naik.to_html(classes="table table-striped", index=False, escape=False),
        turun_html=data_turun.to_html(classes="table table-striped", index=False, escape=False),
        div_html=data_div.to_html(classes="table table-striped", index=False, escape=False),
    )

# =================== RUN APP ===================
if __name__ == '__main__':
    app.run(debug=True)
