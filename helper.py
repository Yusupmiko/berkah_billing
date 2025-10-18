from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np

def get_previous_blth(blth_str, months_back=1):
    date = datetime.strptime(blth_str, '%Y%m')
    prev_date = date - relativedelta(months=months_back)
    return prev_date.strftime('%Y%m')


def copy_dataframe(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini):
    # DataFrames per bulan
    df_lalulalu = pd.DataFrame(lalulalu)
    df_lalu = pd.DataFrame(lalu)
    df_akhir = pd.DataFrame(akhir)
    
    merged = pd.merge(df_lalulalu, df_lalu, on='IDPEL', how='right', suffixes=('_x','_y'))
    merged = pd.merge(merged, df_akhir, on='IDPEL', how='right')
    
    delta = merged['LWBPPAKAI'] - merged['LWBPPAKAI_y']
    
    kroscek = pd.DataFrame({
        'BLTH': blth_kini,
        'IDPEL': merged['IDPEL'],
        'NAMA': merged['NAMA'],
        'TARIF': merged['TARIF'],
        'DAYA': merged['DAYA'].fillna(0).astype(int),
        'SLALWBP': merged['SLALWBP'].fillna(0).astype(int),
        'LWBPCABUT': merged['LWBPCABUT'].fillna(0).astype(int),
        'SELISIH STAN BONGKAR': (merged['SLALWBP'].fillna(0)-merged['LWBPCABUT'].fillna(0)).astype(int),
        'LWBP PASANG': merged['LWBPPASANG'].fillna(0).astype(int),
        'SAHLWBP': merged['SAHLWBP'].fillna(0).astype(int),
        'KWH SEKARANG': merged['LWBPPAKAI'].fillna(0).astype(int),
        'KWH 1 BULAN LALU': merged['LWBPPAKAI_y'].fillna(0).astype(int),
        'KWH 2 BULAN LALU': merged['LWBPPAKAI_x'].fillna(0).astype(int),
        'DELTA PEMKWH': delta.fillna(0).astype(int),
        'DLPD': merged.get('DLPD', pd.Series(0))
    })
    
    # Persentase
    percentage = (delta / merged['LWBPPAKAI_y'].replace(0, np.nan))*100
    percentage = np.nan_to_num(percentage, nan=0, posinf=0, neginf=0)
    kroscek['%'] = pd.Series(percentage).astype(int).astype(str)+'%'
    
    # KET
    kroscek['KET'] = np.where(
        merged['LWBPPAKAI_y'].isna() | (merged['LWBPPAKAI_y']==0),
        'DIV/NA',
        np.where(percentage>=40,'NAIK',
                 np.where(percentage<=-40,'TURUN','AMAN'))
    )
    
    # Link foto
    path_foto1 = 'https://portalapp.iconpln.co.id/acmt/DisplayBlobServlet1?idpel='
    path_foto2 = '&blth='
    kroscek['FOTO AKHIR'] = kroscek['IDPEL'].apply(lambda x: f'<a href="{path_foto1}{x}{path_foto2}{blth_kini}" target="_blank">LINK FOTO</a>')
    kroscek['FOTO LALU'] = kroscek['IDPEL'].apply(lambda x: f'<a href="{path_foto1}{x}{path_foto2}{blth_lalu}" target="_blank">LINK FOTO</a>')
    kroscek['FOTO LALU2'] = kroscek['IDPEL'].apply(lambda x: f'<a href="{path_foto1}{x}{path_foto2}{blth_lalulalu}" target="_blank">LINK FOTO</a>')
    kroscek['FOTO 3BLN'] = kroscek['IDPEL'].apply(lambda x: f'<a href="#" onclick="open3Foto(\'{x}\',\'{blth_kini}\'); return false;">{str(x)[-5:]}</a>')
    
    return kroscek

def naikFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini):
    df = copy_dataframe(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
    return df[df['%'].str.rstrip('%').astype(int)>=40]

def turunFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini):
    df = copy_dataframe(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
    return df[df['%'].str.rstrip('%').astype(int)<=-40]

def divFilter(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini):
    df = copy_dataframe(lalulalu, lalu, akhir, blth_lalulalu, blth_lalu, blth_kini)
    return df[df['KET']=='DIV/NA']
