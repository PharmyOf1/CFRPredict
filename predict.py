import sys, os
from processors import PSI_OBS, update_cfr

#fname = sys.argv[1]
cur_path = os.path.dirname(os.path.realpath(__file__))


if __name__ == '__main__':
    xl = PSI_OBS('samp2.xlsx')
    final_prediction = xl._predict()
    print (final_prediction)
    final_prediction.to_csv(os.path.join(cur_path,'verification.csv'))
