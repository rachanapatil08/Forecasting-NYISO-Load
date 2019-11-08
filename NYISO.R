
rm(list=ls(all=T))

# ETL in R
# data is at http://mis.nyiso.com/public/csv/pal/20190624pal.csv

load_url<-'http://mis.nyiso.com/public/csv/pal/'

# NYISO has up to 10 days
days_back<-10

dates<-seq(Sys.Date(),Sys.Date()-days_back,-1) # reverse sequence of dates

load_urls<-paste(load_url,gsub('-','',dates),'pal.csv',sep='') #urls to process

# Open connection first
require(RPostgreSQL)
pg = dbDriver("PostgreSQL")
conn = dbConnect(drv=pg
                 ,user="electricitymarketwriter"
                 ,password="write123"
                 ,host="localhost"
                 ,port=5432
                 ,dbname="electricitymarket"
)


for(url in load_urls){
  #url<-load_urls[1]
  
  load_csv<-na.omit(read.csv(file=url))
  #load_csv$Time.Stamp<-as.character(as.POSIXct(load_csv$Time.Stamp,format="%m/%d/%Y %H:%M:%S"))
  cat("Processing",url,"...")
  t0<-Sys.time()
  for(k in 1:nrow(load_csv)){
    #k<-1
    stm<-paste(
      'INSERT INTO load VALUES ('
      ,"'",load_csv$Time.Stamp[k],"',"
      ,"'",load_csv$Time.Zone[k],"',"
      ,"'",load_csv$Name[k],"',"
      ,load_csv$PTID[k],","
      #,load_csv$Load[k],");"
      ,load_csv$Load[k],") ON CONFLICT (time_stamp, time_zone, ptid) DO NOTHING;"
      ,sep=""
    )
    
    result<-dbSendQuery(conn,stm) # you can inspect the results here
    
    #dbGetQuery(conn,stm)
  } # of for(k)
  t1<-Sys.time()
  cat('done after',(t1-t0),'s.\n')
  
} # of for(url)

# Close db connection
dbDisconnect(conn)


# Extract and Load Historical Data

# Set to T for monthly process to run
run.monthly<-F

# This is for previous months (zipped)
#http://mis.nyiso.com/public/csv/pal/20190701pal_csv.zip

out_path<-'C:/Temp/NYISO'

months<-seq(as.Date("2019-02-01"), by = "month", length.out = 6)
months<-rev(months)
zipped_load_urls<-paste(load_url,gsub('-','',months),'pal_csv.zip',sep='') #urls to process

# Open connection
require(RPostgreSQL)
pg = dbDriver("PostgreSQL")
conn = dbConnect(drv=pg
                 ,user="electricitymarketwriter"
                 ,password="write123"
                 ,host="localhost"
                 ,port=5432
                 ,dbname="electricitymarket"
)


for(zipped_url in zipped_load_urls){
  if(!run.monthly) break()
  #zipped_url<-zipped_load_urls[1]
  temp_file<-paste(out_path,'/temp.zip',sep="")
  download.file(zipped_url,temp_file) # download archive to a temp file
  unzip(zipfile = temp_file, exdir = out_path) #extract from archive
  file.remove(temp_file) # delete temp file
  
  csvs<-rev(list.files(out_path,full.names = T))
  
  for(csv in csvs){
    #csv<-csvs[1]
    load_csv<-na.omit(read.csv(file=csv))
    #load_csv$Time.Stamp<-as.character(as.POSIXct(load_csv$Time.Stamp,format="%m/%d/%Y %H:%M:%S"))
    
    cat("Processing",csv,"...")
    t0<-Sys.time()
    for(k in 1:nrow(load_csv)){
      #k<-1
      stm<-paste(
        'INSERT INTO load VALUES ('
        ,"'",load_csv$Time.Stamp[k],"',"
        ,"'",load_csv$Time.Zone[k],"',"
        ,"'",load_csv$Name[k],"',"
        ,load_csv$PTID[k],","
        #,load_csv$Load[k],");"
        ,load_csv$Load[k],") ON CONFLICT (time_stamp, time_zone, ptid) DO NOTHING;"
        ,sep=""
      )
      
      result<-dbSendQuery(conn,stm) # you can inspect the results here
      
      #dbGetQuery(conn,stm)
    } # of for(k)
    t1<-Sys.time()
    file.remove(csv)
    cat('done after',(t1-t0),'s.\n')
    
  } # of for(csv)
  
} # of for(url)

# Close db connection
dbDisconnect(conn)

# Forecast average hourly load for the next 24 hours

# setting up time range
from_dt<-'2019-03-01 00:00:00'
to_dt<-'2019-03-24 23:59:59'

#build a a query

qry<-paste(
  "SELECT date_trunc('hour',time_stamp) as ymdh, AVG(total_load) as avg_load
  FROM 
  (SELECT time_stamp, time_zone, SUM(load) as total_load
    FROM load",
  " WHERE time_stamp BETWEEN '",from_dt,"' AND '",to_dt,"'",
  " GROUP BY time_stamp, time_zone) TL
  GROUP BY date_trunc('hour',time_stamp)
  ORDER BY ymdh;",sep=""
)

#retrieve from db
# Open connection
require(RPostgreSQL)
pg = dbDriver("PostgreSQL")
conn = dbConnect(drv=pg
                 ,user="electricitymarketwriter"
                 ,password="write123"
                 ,host="localhost"
                 ,port=5432
                 ,dbname="electricitymarket"
)

hrly_load<-dbGetQuery(conn,qry) #retrieve data

dbDisconnect(conn) # close connection
#check
head(hrly_load)
tail(hrly_load)
nrow(hrly_load)

#make univariate
rownames(hrly_load)<-hrly_load$ymdh
hrly_load$ymdh<-NULL

#check
head(hrly_load)

#plot the last 7 days (24*7 hours)
plot(tail(hrly_load$avg_load,24*7),type='l')

#time series object
require(xts)
hrly_load.xts<-as.xts(hrly_load)

require(ggplot2)
ggplot(hrly_load.xts, aes(x = Index, y = avg_load)) + geom_line()

#using PerformanceAnalytics pkg
require(PerformanceAnalytics)
chart.TimeSeries(hrly_load.xts)

#interactive time-series plot (using dygraphs)
require(dygraphs)
dygraph(hrly_load.xts)

#forecast the next 24 hours
require(forecast)

#using Auto ARIMA
aa<-auto.arima(hrly_load,stepwise = F)
summary(aa)

#forecast
fcst<-forecast(aa,24)
plot(fcst)

#95pct intervals
require(dygraphs)
fcst.95pct<-as.data.frame(fcst)[,c('Lo 95','Point Forecast','Hi 95')]

last.date<-tail(rownames(hrly_load),1)

fcst.hours<-tail(seq(from=as.POSIXct(last.date, tz="America/New_York"), 
    to=as.POSIXct(last.date, tz="America/New_York")+3600*24, by="hour"),-1)

#check
head(fcst.hours)
tail(fcst.hours)
length(fcst.hours)

#plot the forecast
rownames(fcst.95pct)<-fcst.hours
dygraph(fcst.95pct) %>%
  dySeries(c('Lo 95','Point Forecast','Hi 95'))

#plot
fake.fcst.95pct<-as.data.frame(hrly_load)
fake.fcst.95pct$`Lo 95`<-hrly_load$avg_load
fake.fcst.95pct$`Hi 95`<-hrly_load$avg_load
fake.fcst.95pct<-fake.fcst.95pct[,c('Lo 95','avg_load','Hi 95')]
colnames(fcst.95pct)<-c('Lo 95','avg_load','Hi 95')

#bind and plot
dygraph(rbind(fake.fcst.95pct,fcst.95pct)) %>%
  dySeries(c('Lo 95','avg_load','Hi 95'))

new_from_dt<-as.character(as.POSIXct(to_dt)+1)
new_to_dt<-as.character(as.POSIXct(to_dt)+60*60*24)

new_qry<-paste(
  "SELECT date_trunc('hour',time_stamp) as ymdh, AVG(total_load) as avg_load
  FROM 
  (SELECT time_stamp, time_zone, SUM(load) as total_load
    FROM load",
  " WHERE time_stamp BETWEEN '",new_from_dt,"' AND '",new_to_dt,"'",
  " GROUP BY time_stamp, time_zone) TL
  GROUP BY date_trunc('hour',time_stamp)
  ORDER BY ymdh;",sep=""
)

conn = dbConnect(drv=pg
                 ,user="electricitymarketwriter"
                 ,password="write123"
                 ,host="localhost"
                 ,port=5432
                 ,dbname="electricitymarket"
)

new_hrly_load<-dbGetQuery(conn,new_qry) #retrieve data
rownames(new_hrly_load)<-new_hrly_load$ymdh
new_hrly_load$ymdh<-NULL

dbDisconnect(conn) # close connection
#check
head(new_hrly_load)
plot(new_hrly_load$avg_load)

#accuracy
accuracy<-new_hrly_load
accuracy$fcst<-fcst$mean #actual forecast
head(accuracy)

#Mean Absolute Error
accuracy$E<-accuracy$avg_load-accuracy$fcst
accuracy$AE<-abs(accuracy$E)
mae<-mean(accuracy$AE)

#MAPE
accuracy$PE<-accuracy$E/accuracy$avg_load
accuracy$APE<-abs(accuracy$PE)
mape<-mean(accuracy$APE)

#plot
plot(accuracy$avg_load,type='l',xlab=NA,ylab=NA)
par(new=T)
plot(accuracy$fcst,col='blue',xlab=NA,ylab=NA)

require(PerformanceAnalytics)
chart.TimeSeries(accuracy[,c('avg_load','fcst')],legend.loc='bottomright')
chart.TimeSeries(accuracy[,c('E'),drop=F],legend.loc='bottomright')

