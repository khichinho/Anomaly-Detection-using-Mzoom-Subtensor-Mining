import pandas as pd
import numpy as np
# df=pd.read_csv('rvp_35days_data.csv',usecols=['a.return_id','a.request_date','a.return_amount','a.return_reason','b.analytic_category','a.return_from_address_pincode','a.return_action','a.is_fbf_flag','d.spf_claim_reason','c.fraud_area','c.liq_area'])
df=pd.read_csv('rvp_30days_data.csv',usecols=['refund_reason','refund_mode','return_item_shipment_id','user_approved_by','brand','title','analytic_vertical','analytic_sub_category','analytic_category','analytic_super_category','analytic_business_unit','account_age','cms_vertical','is_fbf_flag','marketplace_id','order_date','return_amount','returned_product_id_key','return_from_address_city','return_from_address_pincode','return_from_address_state','account_id','order_item_id','request_date','return_action','return_id','return_item_id','return_item_quantity','return_item_request_date_time','return_item_status','return_reason','return_request_channel','return_sub_reason','seller_id','user_lockin_state','is_alpha_seller','order_payment_type','order_sales_channel','promise_breach','rts_breach'],encoding = "ISO-8859-1")
# df.columns=['is_fbf_flag','request_date','return_action','return_amount','return_from_address_pincode','return_id','return_reason','analytic_category','fraud_area','liq_area ','spf_claim_reason']
df['ret_amnt_bucket']=df['return_amount'].apply(lambda x: '>2000' if x>2000 else '1000-2000' if x>1000 else '750-1000'  if x>750 else '299-750' if x>299 else '0-299' if x<=299 else 'NAN')
df['account_age_bucket']=df['account_age'].apply(lambda x: '>700' if x>700 else '350-700' if x>350 else '180-350'  if x>180 else '90-180' if x>90 else '30-90' if x>30 else '<30' if x<=30 else 'NAN')

#df['diff_ret_del_time_bucket']=df['diff_ret_del_time'].apply(lambda x: '>30' if x>30 else '10-30' if x>10 else '2-10'  if x>2 else '1-2' if x>1 else '0.5-1' if x>0.5 else '0-0.5' if x>=0 else '<0' if x<0 else 'NAN')
df['request_date']=pd.to_datetime(df['request_date'])
from datetime import datetime, timedelta
max_date=datetime.today()
# max_date=max(df['request_date'])
max_date=pd.to_datetime(max_date - timedelta(days=2)).date()
df_7_days=df[(df['request_date']>pd.to_datetime(max_date - timedelta(days=8))) & (df['request_date']<max_date )]
df_7_14_days=df[(df['request_date']>pd.to_datetime(max_date - timedelta(days=15))) & (df['request_date']<=pd.to_datetime(max_date - timedelta(days=8)))  ]
df_14_21_days=df[(df['request_date']>pd.to_datetime(max_date - timedelta(days=22))) & (df['request_date']<=pd.to_datetime(max_date - timedelta(days=15)))  ]
df_21_28_days=df[(df['request_date']>pd.to_datetime(max_date - timedelta(days=29))) & (df['request_date']<=pd.to_datetime(max_date - timedelta(days=22)))  ]
# df_7_days=df[(df['request_date']>pd.to_datetime(datetime.today() - timedelta(days=8))) & (df['request_date']<datetime.today())]
df_last_day=df[ (df['request_date']==max_date )]

ratio_daily_ret_7days=(df_last_day.groupby('request_date').return_id.nunique().mean()/df_7_days.groupby('request_date').return_id.nunique().mean()) if (df_last_day.groupby('request_date').return_id.nunique().mean()/df_7_days.groupby('request_date').return_id.nunique().mean())>1 else 1
ratio_daily_ret_7_14days=(df_last_day.groupby('request_date').return_id.nunique().mean()/df_7_14_days.groupby('request_date').return_id.nunique().mean()) if (df_last_day.groupby('request_date').return_id.nunique().mean()/df_7_14_days.groupby('request_date').return_id.nunique().mean())>1 else 1
ratio_daily_ret_14_21days=(df_last_day.groupby('request_date').return_id.nunique().mean()/df_14_21_days.groupby('request_date').return_id.nunique().mean()) if (df_last_day.groupby('request_date').return_id.nunique().mean()/df_14_21_days.groupby('request_date').return_id.nunique().mean())>1 else 1
ratio_daily_ret_21_28days=(df_last_day.groupby('request_date').return_id.nunique().mean()/df_21_28_days.groupby('request_date').return_id.nunique().mean()) if (df_last_day.groupby('request_date').return_id.nunique().mean()/df_21_28_days.groupby('request_date').return_id.nunique().mean())>1 else 1

print(ratio_daily_ret_7days)
print(ratio_daily_ret_7_14days)
print(ratio_daily_ret_14_21days)
print(ratio_daily_ret_21_28days)



# df_last_day=df[ (df['request_date']==datetime.today() )]
# cols=['refund_reason','refund_mode','brand','title','analytic_vertical','analytic_sub_category','analytic_category','analytic_super_category','analytic_business_unit','account_age','is_fbf_flag','return_from_address_city','return_from_address_pincode','return_from_address_state','account_id','diff_rep_add_flag','return_action','return_reason','return_request_channel','return_sub_reason','seller_id']
#cols=['refund_mode','brand','analytic_vertical','account_age_bucket','return_from_address_state','return_from_address_pincode','return_action','return_reason','return_request_channel','seller_id','ret_amnt_bucket','diff_ret_del_time_bucket','user_lockin_state','is_alpha_seller','order_payment_type','order_sales_channel','promise_breach','rts_breach']

cols=['refund_mode','analytic_vertical','account_age_bucket','return_from_address_state','return_from_address_pincode','return_action','return_reason','return_request_channel','seller_id','ret_amnt_bucket','is_alpha_seller','order_payment_type','order_sales_channel']

# cols=['return_action','ret_amnt_bucket','return_from_address_pincode','return_reason','analytic_category']
import itertools
comb=[]
for L in range(0, len(cols)+1):
    for subset in itertools.combinations(cols, L):
#         print(subset)
        comb.append(subset)
ad=pd.DataFrame()
for i in range(len(comb)):
    if i>0  and len(comb[i])<=5 or (len(comb[i])==1 and comb[i][0] not in ['refund_mode','return_action','return_request_channel','is_alpha_seller','order_payment_type','order_sales_channel']):
        print(ad.shape)
        print((i/len(comb))*100,'%',' completed')
        globals()['df_last_day'+str(i)]=df_last_day.groupby(comb[i]).agg({'return_id':'nunique','return_amount':'sum'}).reset_index()
        l=list(comb[i])
        l.append('request_date')
        j=df_7_days.groupby(l).return_id.nunique().reset_index()
        k=df_7_14_days.groupby(l).return_id.nunique().reset_index()
        m=df_14_21_days.groupby(l).return_id.nunique().reset_index()
        n=df_21_28_days.groupby(l).return_id.nunique().reset_index()
        globals()['df_7_days'+str(i)]=j.groupby(comb[i]).return_id.mean().reset_index()
        globals()['df_7_14_days'+str(i)]=k.groupby(comb[i]).return_id.mean().reset_index()
        globals()['df_14_21_days'+str(i)]=m.groupby(comb[i]).return_id.mean().reset_index()
        globals()['df_21_28_days'+str(i)]=n.groupby(comb[i]).return_id.mean().reset_index()
#         globals()['df_'+str(i)]['prcnt_of_tot']=globals()['df_'+str(i)]['return_id']/total_returns
        globals()['df_last_day'+str(i)]=globals()['df_last_day'+str(i)].merge(globals()['df_7_days'+str(i)], how='left',on=comb[i],suffixes=['_last_day','_7days']).merge(globals()['df_7_14_days'+str(i)],how='left',on=comb[i]).merge(globals()['df_14_21_days'+str(i)],how='left',on=comb[i],suffixes=['_7_14days','_14_21days']).merge(globals()['df_21_28_days'+str(i)],how='left',on=comb[i])
        globals()['df_last_day'+str(i)]['return_id'].fillna(0,inplace=True)
        globals()['df_last_day'+str(i)]['return_id_last_day'].fillna(0,inplace=True)
        globals()['df_last_day'+str(i)]['return_id_7days'].fillna(0,inplace=True)
        globals()['df_last_day'+str(i)]['return_id_7_14days'].fillna(0,inplace=True)
        globals()['df_last_day'+str(i)]['return_id_14_21days'].fillna(0,inplace=True)
#         thresh=np.percentile(globals()['df_'+str(i)]['return_id'],99)

        globals()['ad_'+str(i)]=globals()['df_last_day'+str(i)][((((globals()['df_last_day'+str(i)]['return_id_last_day'])/(globals()['df_last_day'+str(i)]['return_id_7days']+1))>(3*ratio_daily_ret_7days)) | (((globals()['df_last_day'+str(i)]['return_id_last_day'])/(globals()['df_last_day'+str(i)]['return_id_7_14days']+1))>(3*ratio_daily_ret_7_14days)) | (((globals()['df_last_day'+str(i)]['return_id_last_day'])/(globals()['df_last_day'+str(i)]['return_id_14_21days']+1))>(3*ratio_daily_ret_14_21days))  | (((globals()['df_last_day'+str(i)]['return_id_last_day'])/(globals()['df_last_day'+str(i)]['return_id']+1))>(3*ratio_daily_ret_21_28days)) )& ((globals()['df_last_day'+str(i)]['return_id_last_day']>30) | (globals()['df_last_day'+str(i)]['return_amount']>100000))]
        if globals()['ad_'+str(i)].shape[0]>0:
            ad=ad.append(globals()['ad_'+str(i)])
        del l,j,k,m,n,globals()['df_last_day'+str(i)],globals()['df_7_days'+str(i)],globals()['df_7_14_days'+str(i)],globals()['df_14_21_days'+str(i)],globals()['df_21_28_days'+str(i)],globals()['ad_'+str(i)]
#         del globals()['ad_'+str(i)]
#         globals()['ad_'+str(i)]= globals()['df_'+str(i)][(globals()['df_'+str(i)]['return_id']>thresh) & (globals()['df_'+str(i)]['return_id']>10)]

print('iteration completed')
ad['ratio_7days']=ad['return_id_last_day']/(ad['return_id_7days']+1)
ad['ratio_7_14days']=ad['return_id_last_day']/(ad['return_id_7_14days']+1)
ad['ratio_14_21days']=ad['return_id_last_day']/(ad['return_id_14_21days']+1)
ad['ratio_21_28days']=ad['return_id_last_day']/(ad['return_id']+1)
ad['max_ratio']=ad[['ratio_7days','ratio_7_14days','ratio_14_21days','ratio_21_28days']].max(axis=1)
ad['score']=2.5*(ad['ratio_7days'])+2*(ad['ratio_7_14days'])+1.5*(ad['ratio_14_21days'])+(ad['ratio_21_28days'] )


import datetime as dt

#yesterday=dt.datetime.today()-timedelta(days=1)
#yesterday=yesterday.strftime('%Y%m%d') 
#input_file = 'anamoly_detection_{}.csv'.format(yesterday)
#ad_1=pd.read_csv(input_file)
#lists=list(comb[len(comb)-1])
#lists.append('freq')
#ad_1=ad_1[lists]
#ad=ad.merge(ad_1,how='left', on=comb[len(comb)-1] )
#ad['freq']=ad['freq'].fillna(0)
#ad['freq']=ad['freq']+1
today = max_date.strftime('%Y%m%d') 
output_file = 'anamoly_detection_{}.csv'.format(today)
ad=ad.sort_values(by='score',ascending=False)
ad_by_value=ad.sort_values(by='return_amount',ascending=False)
ad_by_count=ad.sort_values(by='return_id_last_day',ascending=False)
ad=ad.head(150000)
ad.to_csv(output_file,index=False)

print('AD file exported successfully')

#cols=list(ad.columns)
cols=['analytic_vertical','return_reason', 'return_from_address_state','return_from_address_pincode']
#cols=['analytic_vertical']
rows=ad['analytic_vertical'].nunique()
ad_summary=pd.DataFrame()
for i in cols:
    ad_summary
#     print(i)

    a=ad[(ad[i].notnull() )][i].unique().tolist()
    b=pd.DataFrame({i:a})
    ad_summary=pd.concat([ad_summary,b.head(rows)], ignore_index=True, axis=1)
    
ad_summary.columns=cols



ad_summary_by_value=pd.DataFrame()
for i in cols:
    ad_summary_by_value
#     print(i)

    a=ad_by_value[(ad_by_value[i].notnull() )][i].unique().tolist()
    b=pd.DataFrame({i:a})
    ad_summary_by_value=pd.concat([ad_summary_by_value,b.head(rows)], ignore_index=True, axis=1)
    
ad_summary_by_value.columns=cols



ad_summary_by_count=pd.DataFrame()
for i in cols:
    ad_summary_by_count
#     print(i)

    a=ad_by_count[(ad_by_count[i].notnull() )][i].unique().tolist()
    b=pd.DataFrame({i:a})
    ad_summary_by_count=pd.concat([ad_summary_by_count,b.head(rows)], ignore_index=True, axis=1)
    
ad_summary_by_count.columns=cols
#del ad_summary['return_id_last_day']
#del ad_summary['return_amount']
#del ad_summary['return_id_7days']
#del ad_summary['return_id_7_14days']
#del ad_summary['return_id_14_21days']
#del ad_summary['return_id']
#del ad_summary['ratio_7days']
#del ad_summary['ratio_7_14days']
#del ad_summary['ratio_14_21days']
#del ad_summary['ratio_21_28days']
#del ad_summary['max_ratio']
#del ad_summary['score']



import sys
import os
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart

COMMASPACE = ', '

a='Anomaly_Detection_{}'.format(today)
def main():
    sender = 'prraj67@gmail.com'
    gmail_password = '5123805051238050'
    recipients = ['priyanshu.raj@flipkart.com','frm-swat-team@flipkart.com','vatsala.tiwari@flipkart.com','sravya.reddy@flipkart.com','suhas.kg@flipkart.com']
    #recipients = ['priyanshu.raj@flipkart.com']
    # Create the enclosing (outer) message
    outer = MIMEMultipart()
    outer['Subject'] = a
    outer['To'] = COMMASPACE.join(recipients)
    outer['From'] = sender
    outer.preamble = 'You will not see this in a MIME-aware mail reader.\n'
    from email.mime.text import MIMEText
    from  IPython.display import HTML
    text = ad_summary.to_html()
    text_val=ad_summary_by_value.to_html()
    text_cnt=ad_summary_by_count.to_html()
    outer.attach(MIMEText('By Score','html'))
    outer.attach(MIMEText(text, 'html'))
    outer.attach(MIMEText('By Value','html'))
    outer.attach(MIMEText(text_val,'html'))
    outer.attach(MIMEText('By Count','html'))
    outer.attach(MIMEText(text_cnt,'html'))


    # List of attachments
    attachments = [output_file]

    # Add the attachments to the message
    for file in attachments:
        try:
            with open(file, 'rb') as fp:
                msg = MIMEBase('application', "octet-stream")
                msg.set_payload(fp.read())
            encoders.encode_base64(msg)
            msg.add_header('Content-Disposition', 'attachment', filename=os.path.basename(file))
            outer.attach(msg)
        except:
            print("Unable to open one of the attachments. Error: ", sys.exc_info()[0])
            raise

    composed = outer.as_string()

    # Send the email
    try:
        with smtplib.SMTP('smtp.gmail.com', 587) as s:
            s.ehlo()
            s.starttls()
            s.ehlo()
            s.login(sender, gmail_password)
            s.sendmail(sender, recipients, composed)
            s.close()
        print("Email sent!")
    except:
        print("Unable to send the email. Error: ", sys.exc_info()[0])
        raise
if __name__ == '__main__':
    main()



