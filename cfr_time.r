##########################################
#CFR Predictor
#Utilizes PSI Data to generate predicted CFR
#Phil Harm - 10/28/2016
#########################################

library(forecast)
path = 'C:\\Users\\UNA0464\\Dropbox\\cfr_file'
fname = 'cfrdata.csv'
data.all = read.csv(file=file.path(path,fname), header=TRUE)
data.all = data.all[complete.cases(data.all),]
d = ts(data.all[,2])
x =auto.arima(d)
fit <- Arima(d, order=c(1,1,1))
fcst = forecast(fit,12)
proj_cfr = as.numeric(fcst$mean)
proj_cfr
