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
from sqlalchemy import text

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


########################################################



@app.route('/dashboard_running_billing', methods=['GET', 'POST'])
def dashboard_running_billing():
    if 'loggedin' not in session:
        return redirect(url_for('login'))

    blth_kini = normalize_blth(request.form.get('blth', datetime.now().strftime('%Y%m')))
    blth_lalu = get_previous_blth(blth_kini, 1)
    blth_lalulalu = get_previous_blth(blth_kini, 2)

    # ====== HAPUS DATA LEBIH DARI 6 BULAN ======
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                DELETE FROM dpm
                WHERE CAST(BLTH AS UNSIGNED) < CAST(
                    DATE_FORMAT(
                        DATE_SUB(
                            STR_TO_DATE(CONCAT(:blth, '01'), '%Y%m%d'),
                            INTERVAL 6 MONTH
                        ),
                        '%Y%m'
                    ) AS UNSIGNED
                )
            """), {"blth": blth_kini})
            conn.commit()
    except Exception as e:
        flash(f"Gagal membersihkan data lama DPM: {e}", "warning")




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
            # Tambahkan dropdown dan textarea HTML ke dataframe
        kroscek['HASIL PEMERIKSAAN'] = kroscek['KET'].apply(lambda x: f'''
        <select class="hasil-pemeriksaan" onfocus="this.options[0].selected = true;">
            <option value="" disabled selected hidden></option>
            <option value="SESUAI" {"selected" if x == "SESUAI" else ""}>SESUAI</option>
            <option value="SALAH STAN" {"selected" if x == "SALAH STAN" else ""}>SALAH STAN</option>
            <option value="TELAT/SALAH PDL" {"selected" if x == "TELAT/SALAH PDL" else ""}>TELAT/SALAH PDL</option>
            <option value="SALAH FOTO" {"selected" if x == "SALAH FOTO" else ""}>SALAH FOTO</option>
            <option value="FOTO BURAM" {"selected" if x == "FOTO BURAM" else ""}>FOTO BURAM</option>
            <option value="LEBIH TAGIH" {"selected" if x == "LEBIH TAGIH" else ""}>LEBIH TAGIH</option>
            <option value="BUKAN FOTO KWH" {"selected" if x == "BUKAN FOTO KWH" else ""}>BUKAN FOTO KWH</option>
            <option value="BENCANA" {"selected" if x == "BENCANA" else ""}>BENCANA</option>
        </select>
    ''')

        kroscek['TINDAK LANJUT'] = '''
        <textarea class="tindak-lanjut" rows="3" cols="30" placeholder="Isi tindak lanjut..."></textarea>
    '''

        kroscek['KETERANGAN'] = '''
        <select class="keterangan" onfocus="this.options[0].selected = true;">
            <option value="" disabled selected hidden></option>
            <option value="3 BULAN TIDAK DAPAT FOTO STAN">3 BULAN TIDAK DAPAT FOTO STAN</option>
            <option value="6 BULAN TIDAK DAPAT FOTO STAN">6 BULAN TIDAK DAPAT FOTO STAN</option>
            <option value="SUDAH BU">SUDAH BU</option>
            <option value="SALAH FOTO">SALAH FOTO</option>
            <option value="720">720</option>
        </select>
    '''


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
        'FOTO AKHIR': Text(),
        'FOTO LALU': Text(),
        'FOTO LALU2': Text(),
        'HASIL PEMERIKSAAN': Text(),
        'TINDAK LANJUT': Text(),
        'KETERANGAN': Text()
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






@app.route("/view_data")
def view_data():
    data_naik = pd.read_sql("SELECT * FROM billing_naik", engine)
    data_turun = pd.read_sql("SELECT * FROM billing_turun", engine)
    data_div = pd.read_sql("SELECT * FROM billing_div", engine)

    # Daftar kolom yang ingin dibersihkan dari newline
    text_columns = ['HASIL PEMERIKSAAN', 'TINDAK LANJUT', 'KETERANGAN']

    for df in [data_naik, data_turun, data_div]:
        for col in text_columns:
            if col in df.columns:
                # Hapus \n dan trim spasi
                df[col] = df[col].astype(str).str.replace(r'[\n\r]+', ' ', regex=True).str.strip()

    return render_template(
        "view_data.html",
        naik_html=data_naik.to_html(classes="table table-striped", index=False, escape=False),
        turun_html=data_turun.to_html(classes="table table-striped", index=False, escape=False),
        div_html=data_div.to_html(classes="table table-striped", index=False, escape=False),
    )

    
@app.route('/update_data', methods=['POST'])
def update_data():
    data = request.get_json()
    idpel = data.get('IDPEL')
    column = data.get('column')
    value = data.get('value')
    table = data.get('table')  # fleksibel: billing, billing_naik, billing_turun, dll

    if not all([idpel, column, table]):
        return jsonify({"error": "Data tidak lengkap"}), 400

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        query = f"UPDATE `{table}` SET `{column}` = %s WHERE IDPEL = %s"
        cursor.execute(query, (value, idpel))
        conn.commit()
        return jsonify({"message": "Data berhasil diperbarui"})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)})
    finally:
        cursor.close()
        conn.close()

    

# =================== RUN APP ===================
if __name__ == '__main__':
    app.run(debug=True)
