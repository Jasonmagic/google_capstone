require(dplyr)
require(ggplot2)

cleaned_csv <- read.csv("C:\\Users\\Jason\\Desktop\\SEC_WFC_BAC_2019-2020_cleaned.csv", 
                        fileEncoding="UTF-8-BOM")
head(cleaned_csv)
 
cleaned_csv$Report_Date <- as.Date(cleaned_csv$Report_Date, format="%m/%d/%Y")
cleaned_csv$Amount <- cleaned_csv$Amount/1e6
cleaned_csv$Amount_QTD <- as.numeric(cleaned_csv$Amount_QTD)/1e6
cleaned_csv$Amount_QTD_prev <- as.numeric(cleaned_csv$Amount_QTD_prev)/1e6

#COVID Time frame
covid_start = as.Date("2019-03-31", format="%Y-%m-%d")
covid_end = as.Date("2020-06-30", format="%Y-%m-%d")
rect <- data.frame(xmin=covid_start, xmax=covid_end, ymin=-Inf, ymax=Inf)

filtered_data <- filter(
  cleaned_csv,GAAP_Code == "NetIncomeLoss" 
  # & cleaned_csv$Ticker == "WFC"
  )

covid_Amount_wfc = c(filtered_data$Amount_QTD[which(filtered_data$Report_Date==covid_start&
                                                       filtered_data$Ticker=="WFC")],
                     filtered_data$Amount_QTD[which(filtered_data$Report_Date==covid_end&
                                                      filtered_data$Ticker=="WFC")])
covid_Amount_bac = c(filtered_data$Amount_QTD[which(filtered_data$Report_Date==covid_start&
                                                      filtered_data$Ticker=="BAC")],
                      filtered_data$Amount_QTD[which(filtered_data$Report_Date==covid_end&
                                                           filtered_data$Ticker=="BAC")])

ggplot(
  data=filtered_data,
  mapping = aes(x = Report_Date,y = Amount_QTD, color=Ticker)
  )+
  
  scale_color_manual(
    values=c("cadetblue4", "orangered")
  )+
  
  geom_point(
  )+
  
  geom_line(
    data=filtered_data[which(filtered_data$Ticker=="WFC"),],
    color = "orangered4"
  )+

  geom_line(
    data=filtered_data[which(filtered_data$Ticker=="BAC"),],
    color = "cadetblue3"
  )+
  
  stat_smooth(
  )+
  
  # facet_grid(
  #   Ticker~.
  # )+
  
  labs(
    title = expression(paste("Quarterly performance (Net Income/Loss) - ", 
                             bold("Bank of America"),
                             " and ", 
                             bold("Wells Fargo")))
  )+
  
  ylab(
    "Amount (in millions)"
  )+
  
  theme_bw(
  )+
  
  theme(
    plot.title = element_text(size = 9.5), 
    axis.text.x = element_text(size = 7.5, angle=25),
    axis.text.y = element_text(size = 7.5),
    axis.title=element_text(size=10)
  )+
  
  scale_x_date(
    name = "Reporting Date", 
    date_labels = "%b-%y", 
    date_breaks = "3 months"
  )+ 
  
  geom_rect(
    data=rect, 
    aes(xmin=xmin, xmax=xmax, ymin=ymin, ymax=ymax),
    color="transparent",
    fill="orange",
    alpha=0.2,
    inherit.aes = FALSE
  )+
  
  geom_text(
    data=cleaned_csv[which(cleaned_csv$Ticker=="BAC"),],
    aes(x=covid_start, y=covid_Amount_bac[1], label=covid_Amount_bac[1]),
    size=3,
    vjust = -2,
    hjust = 0,
    color="cadetblue4"
  )+
  
  geom_text(
    data=cleaned_csv[which(cleaned_csv$Ticker=="WFC"),],
    aes(x=covid_start, y=covid_Amount_wfc[1], label=covid_Amount_wfc[1]),
    size=3,
    vjust = -2,
    hjust = 0,
    color="orangered"
  )+
  
  geom_text(
    data=cleaned_csv[which(cleaned_csv$Ticker=="BAC"),],
    aes(x=covid_end, y=covid_Amount_bac[2], label=covid_Amount_bac[2]),
    size=3,
    vjust = 0,
    hjust = -0.5,
    color="cadetblue4"
  )+
  
  geom_text(
    data=cleaned_csv[which(cleaned_csv$Ticker=="WFC"),],
    aes(x=covid_end, y=covid_Amount_wfc[2], label=covid_Amount_wfc[2]),
    size=3,
    vjust = 0,
    hjust = -0.5,
    color="orangered"
  )
