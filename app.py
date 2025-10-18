from flask import Flask, render_template, request, redirect, url_for, session, flash
from flask_mysqldb import MySQL
import MySQLdb.cursors
import hashlib
from sqlalchemy import create_engine
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
import os
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = 'your_secret_key'

# -------------------- MySQL CONFIG --------------------
app.config['MYSQL_HOST'] = 'localhost'
app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = ''
app.config['MYSQL_DB'] = 'berkah_billing'

mysql = MySQL(app)
engine = create_engine("mysql+pymysql://root:@localhost/berkah_billing")

UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
ALLOWED_EXTENSIONS = {'xlsx', 'xls'}


# -------------------- LOGIN SYSTEM --------------------
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

    # Ambil data semua user dari database
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
    session.clear()  # Hapus semua session
    flash('Anda telah logout', 'success')
    return redirect(url_for('login'))

# -------------------- DASHBOARD --------------------
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

# -------------------- HELPER --------------------
def normalize_blth(blth_str):
    if '-' in blth_str:
        return blth_str.replace('-','')
    elif '/' in blth_str:
        return blth_str.replace('/','')
    else:
        return blth_str

def get_previous_blth(blth_str, months_back=1):
    date = datetime.strptime(blth_str,'%Y%m')
    prev_date = date - relativedelta(months=months_back)
    return prev_date.strftime('%Y%m')




from werkzeug.utils import secure_filename

ALLOWED_EXTENSIONS = {'xlsx', 'xls'}
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.',1)[1].lower() in ALLOWED_EXTENSIONS

# -------------------- DASHBOARD RUNNING BILLING --------------------
@app.route('/dashboard_running_billing', methods=['GET', 'POST'])
def dashboard_running_billing():
    if 'loggedin' not in session:
        return redirect(url_for('login'))

    # Ambil input BLTH dari form (jika tidak ada, pakai bulan sekarang)
    blth_kini = normalize_blth(request.form.get('blth', datetime.now().strftime('%Y%m')))
    blth_lalu = get_previous_blth(blth_kini, 1)
    blth_lalulalu = get_previous_blth(blth_kini, 2)

    # === Jika POST (upload file Excel) ===
    if request.method == 'POST':
        file = request.files.get('file')
        if not file or file.filename == '':
            flash('Tidak ada file yang dipilih', 'danger')
            return redirect(url_for('dashboard_running_billing'))

        if not allowed_file(file.filename):
            flash('Format file tidak didukung (hanya .xlsx atau .xls)', 'danger')
            return redirect(url_for('dashboard_running_billing'))

        filename = secure_filename(file.filename)
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        file.save(filepath)

        try:
            df_upload = pd.read_excel(filepath)
            df_upload.columns = [c.strip().upper() for c in df_upload.columns]

            # Pastikan semua kolom utama ada
            expected = {'BLTH', 'IDPEL', 'NAMA', 'TARIF', 'DAYA', 'SLALWBP', 
                        'LWBPCABUT', 'LWBPPASANG', 'SAHLWBP', 'LWBPPakai', 'DLPD'}
            missing = expected - set(df_upload.columns)
            if missing:
                flash(f'Kolom berikut tidak ditemukan di Excel: {missing}', 'danger')
                return redirect(url_for('dashboard_running_billing'))

            df_upload['BLTH'] = blth_kini  # paksa isi BLTH sesuai bulan dipilih

            # Simpan ke tabel DPM
            df_upload.to_sql('dpm', engine, if_exists='append', index=False)
            flash(f'File {filename} berhasil diupload dan disimpan ke database.', 'success')

        except Exception as e:
            flash(f'Gagal memproses file: {e}', 'danger')
            return redirect(url_for('dashboard_running_billing'))

    # === Ambil data DPM 3 bulan terakhir ===
    try:
        df = pd.read_sql(
            "SELECT * FROM dpm WHERE BLTH IN (%s, %s, %s)",
            engine,
            params=(blth_kini, blth_lalu, blth_lalulalu)
        )
    except Exception as e:
        flash(f"Gagal membaca data DPM: {e}", 'danger')
        return render_template('dashboard_running_billing.html', naik=[], turun=[], div=[])

    if df.empty:
        flash("Belum ada data DPM untuk periode ini.", "info")
        return render_template('dashboard_running_billing.html', naik=[], turun=[], div=[])

    # === Hitung perbandingan antar bulan ===
    df.columns = [c.upper() for c in df.columns]
    df_kini = df[df['BLTH'] == blth_kini]
    df_lalu = df[df['BLTH'] == blth_lalu]

    merged = df_kini.merge(df_lalu[['IDPEL', 'SAHLWBP']], on='IDPEL', how='left', suffixes=('_KINI', '_LALU'))
    merged['SAHLWBP_LALU'] = merged['SAHLWBP_LALU'].fillna(0)
    merged['DELTA_KWH'] = merged['SAHLWBP_KINI'] - merged['SAHLWBP_LALU']
    merged['PERSEN'] = np.where(merged['SAHLWBP_LALU'] > 0, 
                                (merged['DELTA_KWH'] / merged['SAHLWBP_LALU']) * 100, 0)

    naik_df = merged[merged['PERSEN'] >= 40]
    turun_df = merged[merged['PERSEN'] <= -40]
    div_df = merged[(merged['SAHLWBP_KINI'].isna()) | (merged['SAHLWBP_LALU'].isna())]

    # === Simpan hasil ke DB (opsional) ===
    try:
        naik_df.to_sql('billing_naik', engine, if_exists='replace', index=False)
        turun_df.to_sql('billing_turun', engine, if_exists='replace', index=False)
        div_df.to_sql('billing_div', engine, if_exists='replace', index=False)
    except Exception as e:
        flash(f"Gagal menyimpan hasil ke database: {e}", "warning")

    return render_template(
        'dashboard_running_billing.html',
        naik=naik_df.to_dict(orient='records'),
        turun=turun_df.to_dict(orient='records'),
        div=div_df.to_dict(orient='records'),
        blth_terakhir=blth_kini
    )


# -------------------- VIEW DATA --------------------
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


if __name__ == '__main__':
    app.run(debug=True)
