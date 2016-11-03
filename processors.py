import csv, os, xlrd, pickle
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

class PSI_OBS(object):
    def __init__(self,xl_file):
        self.fname = os.path.split(xl_file)[1]
        self.was_burned = False

        #Load/Clean Data
        tab = 'Output' if 'Output' in xlrd.open_workbook(xl_file, on_demand=True).sheet_names() else 'Total'
        data = pd.ExcelFile(xl_file).parse(tab)

        #Specify Column Numbers for Further Transformation
        headers = list(data)
        lag_0 = headers.index([d for d in headers if isinstance(d,datetime)][0])
        cfr_date = headers[lag_0].date()
        sku_col = headers.index([s for s in headers if s in ['Aggregate Column', 'Item Code','ItemCode']][0])
        dos_targ_col = headers.index('Target') if 'Target' in headers else 0
        measure_col = headers.index('Measure') if 'Measure' in headers else 2

        #Drop NaN's in SKU Col
        data = data[np.isfinite(data.iloc[:,sku_col])]

        #Potential Fix for Incorrect PSI Formats
        data = self.__fix_for_outdated(data,sku_col)

        #Setup Columns for Extraction and get new variables DF
        data['VARIABLE'] = data.iloc[:,sku_col].map(int).map(str) + "_" + data.iloc[:,measure_col]
        for r in range(13):
            data['LAG{}'.format(r)] = data.iloc[:,lag_0+r]
        #data['LAG0'], data['LAG1'] = data.iloc[:,lag_0], data.iloc[:,lag_0+1]
        #data['LAG2'], data['LAG3'] = data.iloc[:,lag_0+2],data.iloc[:,lag_0+3]
        added_variables = self.__add_additional_variables(data, lag_0, dos_targ_col, measure_col,cfr_date)

        #Transpose 2 Columns
        self.psi_data = data[['VARIABLE','LAG0','LAG1','LAG2','LAG3','LAG4','LAG5','LAG6','LAG7']]
        psi_covdur = self.psi_data[self.psi_data['VARIABLE'].str.endswith(('CovDur','ProjOH'))]
        psi_covdur = psi_covdur.T.reset_index()
        psi_covdur = psi_covdur.rename(columns = psi_covdur.iloc[0])[1:]
        psi_covdur.reset_index()

        #Append New Variables for final data row
        self.psi_covdur = psi_covdur.reset_index().join(added_variables, how="left").set_index('index')

        #For Prediction
        self.pred_12_weeks = data.ix[:,'VARIABLE':]
        self.psi_date = cfr_date

    def __add_additional_variables(self, df, x, y, z, cfr_date):
        base = df[df['VARIABLE'].str.endswith("CovDur")]
        a = base.iloc[:,x] - base.iloc[:,y]
        num_below = len([x for x in a if x < 0])
        percent_below = num_below/len(a)
        iso_lag = lambda x: cfr_date + timedelta(days=7*x)
        iso_week = ['{}{num:02d}'.format(iso_lag(i).isocalendar()[0],num=iso_lag(i).isocalendar()[1]) for i in range(8)]
        quarter_week = [iso_lag(i).isocalendar()[1]%13 for i in range(8)]
        cfr_week = [[val for key, val in CFR().history.items() if key in iso_week[i]][0] for i in range(8)]
        return pd.DataFrame([[num_below,percent_below,iso_week[0],quarter_week[0],cfr_week[0]], #Lag0
                             [num_below,percent_below,iso_week[1],quarter_week[1],cfr_week[1]], #Lag1
                             [num_below,percent_below,iso_week[2],quarter_week[2],cfr_week[2]], #Lag2
                             [num_below,percent_below,iso_week[3],quarter_week[3],cfr_week[3]], #Lag3
                             [num_below,percent_below,iso_week[4],quarter_week[4],cfr_week[4]], #Lag4
                             [num_below,percent_below,iso_week[5],quarter_week[5],cfr_week[5]], #Lag5
                             [num_below,percent_below,iso_week[6],quarter_week[6],cfr_week[6]], #Lag6
                             [num_below,percent_below,iso_week[7],quarter_week[7],cfr_week[7]], #Lag7
                            ],columns=('BELOW_TARG','PERCENT_TARG','DATE','QUARTER_WEEK','CFR'))

    def burn_to_dataset(self, df, w_path):
        if df['CFR'].iloc[0] == 0:
            print ('Did not burn {}, full data not yet avaiable'.format(self.fname))
            return
        if not os.path.exists(w_path):
            open(w_path, 'w').close()
        try:
            current = pd.read_csv(w_path)
            merged = current.merge(df, how='outer')
            merged.to_csv(w_path, index=False, header=True)
            print ('Burned: {}'.format(self.fname))
        except Exception as e:
            if str(e) == "No columns to parse from file":
                df.to_csv(w_path,index=False,header=True)
                print ('First Burned: {}'.format(self.fname))
        self.was_burned = True

    def __fix_for_outdated(self, df, sku_col):
        return df

    def _predict(self):
        from cfr_predict import Prediction
        pred_data = self.pred_12_weeks
        cout = Prediction(pred_data,self.psi_date)
        return cout.pred_model





class CFR(object):
    def __init__(self):
        self.cfr_path = os.path.join(os.getcwd(),'cfr.pickle')
        if not os.path.exists(self.cfr_path):
            with open(cfr_path, 'wb') as tmp:
                pickle.dump(dict(),tmp)

        with open(self.cfr_path, 'rb') as pk:
            self.history = (pickle.load(pk))

def update_cfr(csv_file):
    with open(csv_file, 'r') as infile:
        reader = csv.reader(infile)
        next(reader)
        d = {rows[0]:rows[1] for rows in reader}
        for k,v in d.items():
            if v == '':
                d[k] = 0.0
            else:
                d[k] = float(v)

    with open(os.path.join(os.getcwd(),'cfr.pickle'), 'wb') as tmp:
        pickle.dump(d,tmp)
        print ('CFR file refreshed.')



#Remove In/Outs if it has 3650s days
