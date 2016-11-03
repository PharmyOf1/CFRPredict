import pandas as pd
from datetime import datetime, timedelta
from r_models import models
import subprocess, os

class Prediction(object):
	def __init__(self,observation,cfr_date):
		self.observation = observation
		self.observation = self.observation.T.reset_index()
		self.observation = self.observation.rename(columns = self.observation.iloc[0])[1:]
		self.observation.reset_index()
		self.pred_model = self._run_algo(cfr_date)


	def _run_algo(self,cfr_date):
		temp = []
		for i in range(1,len(models)+1):
			intercept, coeffs = self.__filter_r_string(i)
			filtered = self.observation[list(coeffs.keys())]
			num_rows = filtered.shape[0]
			algorithm = float(intercept)
			for x in range(1,num_rows):
				for c in filtered:
					col = filtered[c]
					val = float(col[x])
					algorithm  = algorithm + (coeffs[c]*val)
				algorithm = .99 if algorithm > .99 else algorithm
				alogorithm = .9 if algorithm <.9 else algorithm
				temp.append(['Model {}'.format(i), cfr_date + timedelta(days=7*(x-1)), round(algorithm,4)*100])
		all_models = pd.DataFrame(temp,columns=['MODEL','DATE', 'CFR'])
		all_models = pd.pivot_table(all_models, values = 'CFR', index=['DATE'], columns = 'MODEL').reset_index()

		command = 'C:\\Users\\UNA0464\\Documents\\R\\R-3.3.1\\bin\\Rscript'
		path2script = os.path.join(os.getcwd(),'cfr_time.r')
		cmd = [command, path2script]
		ts_fcst = [round(float(x)*100,2) for x in subprocess.check_output(cmd,universal_newlines=True,shell=True).split() if '[' not in x]

		all_models['Model {}'.format(len(models)+1)] = ts_fcst
		all_models['PRED'] = all_models.mean(axis=1)

		#Convert to Percent
		for mod in range(1,len(models)+2):
			all_models['Model {}'.format(mod)] = all_models['Model {}'.format(mod)].astype(str).map('{}%'.format)
		all_models['PRED'] = all_models['PRED'].astype(str).map('{}%'.format)

		return all_models

	def __filter_r_string(self,mod_num):
		r_string = models[mod_num].split()
		r_string = [x for x in r_string if len(x)>4]
		for x,y in enumerate(r_string):
			if y.startswith('X'):
				z = y[1:]
				r_string[x] = z
		i = iter(r_string)
		coeffs = dict(zip(i,i))
		intercept = coeffs['(Intercept)']
		coeffs.pop('(Intercept)',None)
		for k,v in coeffs.items():
			coeffs[k] = float(v)
		return (intercept, coeffs)
