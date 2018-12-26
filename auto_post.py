from tornado import ioloop
from tornado import gen, web
import datetime
from datetime import timedelta
import pprint
import csv
import ujson as json
import re
import requests
import ast
import subprocess


#Configuration vars
start = 3
end = 12

csv_files = {'lead_part_1.csv':1271, 'lead_part_2.csv':0, 'lead_part_3.csv':0,
             'lead_part_4.csv':0, 'lead_part_5.csv':0, 'lead_part_6.csv':0,
             'lead_part_7.csv':0, 'lead_part_8.csv':0, 'lead_part_9.csv':0}

url = 'https://devlb.spartanapproach.com/leads/'
von_count = 0

def hour_in_range(start, end, x):
    """Return true if x is in the range [start, end]"""
    if start <= end:
        return start <= x < end
    else:
        return start <= x or x < end

@gen.coroutine
def post_csv():
    loop = ioloop.IOLoop.current()
    for file_name in csv_files:
        von_count = 0
        with open(file_name, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                ooo = {}
                von_count = von_count + 1
                if von_count > csv_files[file_name]:
                    for key in row.keys():
                        #if key == 'comments' or key == 'call_count' or key == 'call_history':
                            #print("{}: {}: {}".format(key, row[key], type(row[key])))
                        #*** Este if decide todo lo que vamos a omitir
                        if key and key != 'labels' and key != 'None' and key != 'motorhome_effective_date' and key != 'health_enrolled_all_timestamp' and key != 'zip' and key != 'call_count' and key != 'final_premium_after_subsidy_2018' and key != 'created_at_timestamp' and key != 'last_update_at' and key != 'watchers' and key != 'created_date' and row[key] and row[key] != b'' and row[key] != "b''":
                        #if ((key == 'comments' or key == 'call_count' or key == 'call_history') and row[key] != "b''"):
                            #*** Trata de convertir a lista. Si falla o cae en el if interno, lo convierte a string
                            try:
                                val = ast.literal_eval(eval(row[key][1:]))
                                ooo[key] = ast.literal_eval(eval(row[key][1:]))
                                if(key == 'call_count' and type(ooo[key]) == type(1)):
                                    l = []
                                    l.append(ooo[key])
                                    ooo[key] = l
                                if(key == "total_individual_income" or key == "bank_account_number" or key == "final_subsidy_2018" or key == "total_income_used_on_quote" or key == "application_2017" or key == "auto_bound_timestamp" or key == "final_subsidy_2018" or key == "home_transfer_received_timestamp" or key == "number_of_dependent_children_in_house" or key == "health_tentative_timestamp" or key == 'renewal_complete_timestamp' or key == 'total_household_income' or key == "health_presold_timestamp" or key == 'health_enrolled_paid_timestamp' or key == 'final_gross_premium_2018' or key == 'health_enrolled_timestamp' or key == 'health_active_timestamp' or key == 'transfer_timestamp' or key == "health_presold_timestamp"):
                                    ooo[key] = ast.literal_eval(str(row[key]))
                                #print("Converted value: {} / Converted Type: {}".format(ooo[key], type(ooo[key])))
                            except:
                                ooo[key] = ast.literal_eval(str(row[key]))
                    if 'health_lead_status' in ooo:
                        if ooo['health_lead_status'] == b'2018 welcome call'\
                        or ooo['health_lead_status'] == b'2017 welcome call'\
                        or ooo['health_lead_status'] == b'2016 welcome call'\
                        or ooo['health_lead_status'] == b'2018 welcomecall'\
                        or ooo['health_lead_status'] == b'2017 welcomecall'\
                        or ooo['health_lead_status'] == b'2016 welcomecall':
                            ooo['health_lead_status'] = 'welcome call'
                        elif ooo['health_lead_status'] == b'2018 turning 65'\
                        or ooo['health_lead_status'] == b'turning 65':
                            ooo['health_lead_status'] = 'TURNING 65'
                        elif ooo['health_lead_status'] == b'2018 transferred'\
                        or ooo['health_lead_status'] == b'2016 previously transferred'\
                        or ooo['health_lead_status'] == b'2017 transferred'\
                        or ooo['health_lead_status'] == b'prior transfer'\
                        or ooo['health_lead_status'] == b'transfered'\
                        or ooo['health_lead_status'] == b'prior transfer'\
                        or ooo['health_lead_status'] == b'transfered'\
                        or ooo['health_lead_status'] == b'cs transfer'\
                        or ooo['health_lead_status'] == b'2017 transferred'\
                        or ooo['health_lead_status'] == b'2017 transferred':
                            ooo['health_lead_status'] = 'TRANSFERRED'
                        elif ooo['health_lead_status'] == b'2018 tentative'\
                        or ooo['health_lead_status'] == b'2017 tentative'\
                        or ooo['health_lead_status'] == b'2016 tentative'\
                        or ooo['health_lead_status'] == b'tentative':
                            ooo['health_lead_status'] = 'TENTATIVE'
                        elif ooo['health_lead_status'] == b'2018 renewal complete'\
                        or ooo['health_lead_status'] == b'2018 renewal - life change complete'\
                        or ooo['health_lead_status'] == b'2016 renewal complete - active'\
                        or ooo['health_lead_status'] == b'2016 renewal cancelled'\
                        or ooo['health_lead_status'] == b'2017 renewal problem'\
                        or ooo['health_lead_status'] == b'2016 renewal complete - to be reviewed'\
                        or ooo['health_lead_status'] == b'2017 renewal - no 2016 app'\
                        or ooo['health_lead_status'] == b'2017 reactive renewal complete'\
                        or ooo['health_lead_status'] == b'2018 reactive renewal complete'\
                        or ooo['health_lead_status'] == b'2018 renewal - application problem'\
                        or ooo['health_lead_status'] == b'2017 renewal - no 2017 app'\
                        or ooo['health_lead_status'] == b'2016 email response only'\
                        or ooo['health_lead_status'] == b'2016 renewal complete - not active'\
                        or ooo['health_lead_status'] == b'2017 renewal - client changes'\
                        or ooo['health_lead_status'] == b'2017 renewal - not enough info'\
                        or ooo['health_lead_status'] == b'2018 renewal assigned'\
                        or ooo['health_lead_status'] == b'2017 reactive renewal pending'\
                        or ooo['health_lead_status'] == b'2018 enrolled-renewal-no payment made'\
                        or ooo['health_lead_status'] == b'2018 ready to renew - oos'\
                        or ooo['health_lead_status'] == b'2016 renewal- no id card'\
                        or ooo['health_lead_status'] == b'2016 renewal - no application'\
                        or ooo['health_lead_status'] == b'2017 to be renewed'\
                        or ooo['health_lead_status'] == b'2016 renewal life change'\
                        or ooo['health_lead_status'] == b'2018 renewal - no life change'\
                        or ooo['health_lead_status'] == b'2016 renewal- in force 2015 app only'\
                        or ooo['health_lead_status'] == b'2018 reactive renewal presold'\
                        or ooo['health_lead_status'] == b'2016 renewal- in force no app'\
                        or ooo['health_lead_status'] == b'2016 renewal email response received'\
                        or ooo['health_lead_status'] == b'2016 renewal pending'\
                        or ooo['health_lead_status'] == b'2016 renewal voicemail received'\
                        or ooo['health_lead_status'] == b'2017 reactive renewal presold'\
                        or ooo['health_lead_status'] == b'february renewals'\
                        or ooo['health_lead_status'] == b'renewals-pending':
                            if ooo['health_lead_status'] == b'2018 renewal complete':
                                ooo['health_lead_status_details'] = 'Complete'
                            elif ooo['health_lead_status'] == b'2018 renewal - life change complete':
                                ooo['health_lead_status_details'] = 'LIFE CHANGE COMPLETE'
                            elif ooo['health_lead_status'] == b'2016 renewal complete - active':
                                ooo['health_lead_status_details'] = 'Complete'
                            elif ooo['health_lead_status'] == b'2016 renewal cancelled':
                                ooo['health_lead_status_details'] = 'WILL NOT RENEW'
                            elif ooo['health_lead_status'] == b'2017 renewal problem':
                                ooo['health_lead_status_details'] = 'APPLICATION PROBLEM'
                            elif ooo['health_lead_status'] == b'2016 renewal complete - to be reviewed':
                                ooo['health_lead_status_details'] = 'COMPLETE'
                            elif ooo['health_lead_status'] == b'2017 renewal - no 2016 app':
                                ooo['health_lead_status_details'] = 'PENDING'
                            elif ooo['health_lead_status'] == b'2017 reactive renewal complete':
                                ooo['health_lead_status_details'] = 'COMPLETE'
                            elif ooo['health_lead_status'] == b'2018 reactive renewal complete':
                                ooo['health_lead_status_details'] = 'COMPLETE'
                            elif ooo['health_lead_status'] == b'2018 renewal - application problem':
                                ooo['health_lead_status_details'] = 'APPLICATION PROBLEM'
                            elif ooo['health_lead_status'] == b'2017 renewal - no 2017 app':
                                ooo['health_lead_status_details'] = 'PENDING'
                            elif ooo['health_lead_status'] == b'2016 email response only':
                                ooo['health_lead_status_details'] = 'PENDING'
                            elif ooo['health_lead_status'] == b'2016 renewal complete - not active':
                                ooo['health_lead_status_details'] = 'COMPLETE'
                            elif ooo['health_lead_status'] == b'2017 renewal - client changes':
                                ooo['health_lead_status_details'] = 'CLIENT CHANGES'
                            elif ooo['health_lead_status'] == b'2017 renewal - not enough info':
                                ooo['health_lead_status_details'] = 'APPLICATION PROBLEM'
                            elif ooo['health_lead_status'] == b'2018 renewal assigned':
                                ooo['health_lead_status_details'] = 'ASSIGNED'
                            elif ooo['health_lead_status'] == b'2017 reactive renewal pending':
                                ooo['health_lead_status_details'] = 'PENDING'
                            elif ooo['health_lead_status'] == b'2018 enrolled-renewal-no payment made':
                                ooo['health_lead_status_details'] = 'PRESOLD'
                            elif ooo['health_lead_status'] == b'2018 ready to renew - oos':
                                ooo['health_lead_status_details'] = 'PRESOLD'
                            elif ooo['health_lead_status'] == b'2016 renewal- no id card':
                                ooo['health_lead_status_details'] = 'APPLICATION PROBLEM'
                            elif ooo['health_lead_status'] == b'2016 renewal - no application':
                                ooo['health_lead_status_details'] = 'APPLICATION PROBLEM'
                            elif ooo['health_lead_status'] == b'2017 to be renewed':
                                ooo['health_lead_status_details'] = 'PENDING'
                            elif ooo['health_lead_status'] == b'2016 renewal life change':
                                ooo['health_lead_status_details'] = 'LIFE CHANGE'
                            elif ooo['health_lead_status'] == b'2018 renewal - no life change':
                                ooo['health_lead_status_details'] = 'NO LIFE CHANGE'
                            elif ooo['health_lead_status'] == b'2016 renewal- in force 2015 app only':
                                ooo['health_lead_status_details'] = 'PENDING'
                            elif ooo['health_lead_status'] == b'2018 reactive renewal presold':
                                ooo['health_lead_status_details'] = 'PRESOLD'
                            elif ooo['health_lead_status'] == b'2016 renewal- in force no app':
                                ooo['health_lead_status_details'] = 'PENDING'
                            elif ooo['health_lead_status'] == b'2016 renewal email response received':
                                ooo['health_lead_status_details'] = 'PENDING'
                            elif ooo['health_lead_status'] == b'2016 renewal pending':
                                ooo['health_lead_status_details'] = 'PENDING'
                            elif ooo['health_lead_status'] == b'2016 renewal voicemail received':
                                ooo['health_lead_status_details'] = 'PENDING'
                            elif ooo['health_lead_status'] == b'2017 reactive renewal presold':
                                ooo['health_lead_status_details'] = 'PRESOLD'
                            elif ooo['health_lead_status'] == b'february renewals':
                                ooo['health_lead_status_details'] = 'PENDING'
                            elif ooo['health_lead_status'] == b'renewals-pending':
                                ooo['health_lead_status_details'] = 'Pending'
                            else:
                                ooo['health_lead_status_details'] = ''
                            ooo['health_lead_status'] = 'RENEWAL'
                        elif ooo['health_lead_status'] == b'2017 oep marketing email received'\
                        or ooo['health_lead_status'] == b'2017 need sep'\
                        or ooo['health_lead_status'] == b'2017 presold-stm'\
                        or ooo['health_lead_status'] == b'2016 presold'\
                        or ooo['health_lead_status'] == b'emailed application'\
                        or ooo['health_lead_status'] == b'in process/follow up'\
                        or ooo['health_lead_status'] == b'2018 presold'\
                        or ooo['health_lead_status'] == b'ready to enroll'\
                        or ooo['health_lead_status'] == b'presold'\
                        or ooo['health_lead_status'] == b'sold'\
                        or ooo['health_lead_status'] == b'2017 presold'\
                        or ooo['health_lead_status'] == b'2017 presold has fl blue'\
                        or ooo['health_lead_status'] == b'2017 presold-sep':
                            if ooo['health_lead_status'] == b'2017 oep marketing email received':
                                ooo['health_lead_status_details'] = 'READY TO ENROLL'
                            elif ooo['health_lead_status'] == b'2017 need sep':
                                ooo['health_lead_status_details'] = 'READY TO ENROLL'
                            elif ooo['health_lead_status'] == b'2017 presold-stm':
                                ooo['health_lead_status_details'] = 'READY TO ENROLL'
                            elif ooo['health_lead_status'] == b'2016 presold':
                                ooo['health_lead_status_details'] = 'READY TO ENROLL'
                            elif ooo['health_lead_status'] == b'emailed application':
                                ooo['health_lead_status_details'] = 'READY TO ENROLL'
                            elif ooo['health_lead_status'] == b'in process/follow up':
                                ooo['health_lead_status_details'] = 'READY TO ENROLL'
                            elif ooo['health_lead_status'] == b'2018 presold':
                                ooo['health_lead_status_details'] = 'READY TO ENROLL'
                            elif ooo['health_lead_status'] == b'ready to enroll':
                                ooo['health_lead_status_details'] = 'READY TO ENROLL'
                            elif ooo['health_lead_status'] == b'presold':
                                ooo['health_lead_status_details'] = 'READY TO ENROLL'
                            elif ooo['health_lead_status'] == b'sold':
                                ooo['health_lead_status_details'] = 'READY TO ENROLL'
                            elif ooo['health_lead_status'] == b'2017 presold':
                                ooo['health_lead_status_details'] = 'READY TO ENROLL'
                            elif ooo['health_lead_status'] == b'2017 presold has fl blue':
                                ooo['health_lead_status_details'] = 'READY TO ENROLL'
                            elif ooo['health_lead_status'] == b'2017 presold-sep':
                                ooo['health_lead_status_details'] = 'READY TO ENROLL'
                            else:
                                ooo['health_lead_status_details'] = ''
                            ooo['health_lead_status'] = 'PRESOLD'
                        elif ooo['health_lead_status'] == b'2018 pending'\
                        or ooo['health_lead_status'] == b'2017 pending'\
                        or ooo['health_lead_status'] == b'pending'\
                        or ooo['health_lead_status'] == b'2016 pending':
                            ooo['health_lead_status'] = 'PENDING'
                        elif ooo['health_lead_status'] == b'2018 new lead'\
                        or ooo['health_lead_status'] == b'new'\
                        or ooo['health_lead_status'] == b'2017 new lead'\
                        or ooo['health_lead_status'] == b'new-auto':
                            ooo['health_lead_status'] = 'NEW'
                            ooo['health_lead_status_details'] = ''
                        elif ooo['health_lead_status'] == b'2018 enrolled - paid'\
                        or ooo['health_lead_status'] == b'2018 not submitted - long app'\
                        or ooo['health_lead_status'] == b'2018 enrolled'\
                        or ooo['health_lead_status'] == b'2018 enrolled - different from quote'\
                        or ooo['health_lead_status'] == b'2017 enrolled payment error'\
                        or ooo['health_lead_status'] == b'2017 enrolled-paid'\
                        or ooo['health_lead_status'] == b'2016 enrolled-paid'\
                        or ooo['health_lead_status'] == b'2018 enrolled - client paid'\
                        or ooo['health_lead_status'] == b'enrolled-paid'\
                        or ooo['health_lead_status'] == b'enrolled'\
                        or ooo['health_lead_status'] == b'2018 enrolled-stm'\
                        or ooo['health_lead_status'] == b'2016 enrolled'\
                        or ooo['health_lead_status'] == b'2017 enrolled'\
                        or ooo['health_lead_status'] == b'old enrolled'\
                        or ooo['health_lead_status'] == b'2018 failed enrollment - app made'\
                        or ooo['health_lead_status'] == b'2018 payment fixed - ready to submit'\
                        or ooo['health_lead_status'] == b'pre-enrolled'\
                        or ooo['health_lead_status'] == b'2018 enrollment error'\
                        or ooo['health_lead_status'] == b'2018 enrolled payment declined'\
                        or ooo['health_lead_status'] == b'2018 failed enrollment - 3-way complete'\
                        or ooo['health_lead_status'] == b'2018 failed enrollment - need app'\
                        or ooo['health_lead_status'] == b'2017 enrolled-payment denied'\
                        or ooo['health_lead_status'] == b'2017 enrolled-second payment error'\
                        or ooo['health_lead_status'] == b'2018 enrolled - payment fixed'\
                        or ooo['health_lead_status'] == b'2018 enrolled no payment info'\
                        or ooo['health_lead_status'] == b'2017 enrollment error'\
                        or ooo['health_lead_status'] == b'2015 marketplace application'\
                        or ooo['health_lead_status'] == b'2016 needs to be corrected':
                            if ooo['health_lead_status'] == b'2018 enrolled - paid':
                                ooo['health_lead_status_details'] = 'Paid'
                            elif ooo['health_lead_status'] == b'2018 enrolled':
                                ooo['health_lead_status_details'] = 'missing payment info'
                            elif ooo['health_lead_status'] == b'2018 enrolled - different from quote':
                                ooo['health_lead_status_details'] = 'DIFFERENT FROM QUOTE'
                            elif ooo['health_lead_status'] == b'2017 enrolled payment error':
                                ooo['health_lead_status_details'] = 'PAYMENT ERROR'
                            elif ooo['health_lead_status'] == b'2017 enrolled-paid':
                                ooo['health_lead_status_details'] = 'PAID'
                            elif ooo['health_lead_status'] == b'2016 enrolled-paid':
                                ooo['health_lead_status_details'] = 'PAID'                    
                            elif ooo['health_lead_status'] == b'2018 enrolled - client paid':
                                ooo['health_lead_status_details'] = 'PAID'
                            elif ooo['health_lead_status'] == b'enrolled-paid':
                                ooo['health_lead_status_details'] = 'PAID'
                            elif ooo['health_lead_status'] == b'enrolled':
                                ooo['health_lead_status_details'] = 'missing payment info'
                            elif ooo['health_lead_status'] == b'2018 enrolled-stm':
                                ooo['health_lead_status_details'] = 'paid'
                            elif ooo['health_lead_status'] == b'2016 enrolled':
                                ooo['health_lead_status_details'] = 'paid'
                            elif ooo['health_lead_status'] == b'2017 enrolled':
                                ooo['health_lead_status_details'] = 'paid'
                            elif ooo['health_lead_status'] == b'old enrolled':
                                ooo['health_lead_status_details'] = 'paid'
                            elif ooo['health_lead_status'] == b'2018 failed enrollment - app made':
                                ooo['health_lead_status_details'] = 'DIFFERENT FROM QUOTE'
                            elif ooo['health_lead_status'] == b'2018 payment fixed - ready to submit':
                                ooo['health_lead_status_details'] = 'PAYMENT FIXED'
                            elif ooo['health_lead_status'] == b'2018 enrollment error':
                                ooo['health_lead_status_details'] = 'DIFFERENT FROM QUOTE'
                            elif ooo['health_lead_status'] == b'2018 enrolled payment declined':
                                ooo['health_lead_status_details'] = 'PAYMENT DECLINED'
                            elif ooo['health_lead_status'] == b'2017 enrolled-payment denied':
                                ooo['health_lead_status_details'] = 'PAYMENT DECLINED'
                            elif ooo['health_lead_status'] == b'2017 enrolled-second payment error':
                                ooo['health_lead_status_details'] = 'PAYMENT DECLINED'
                            elif ooo['health_lead_status'] == b'2018 enrolled - payment fixed':
                                ooo['health_lead_status_details'] = 'PAYMENT FIXED'
                            elif ooo['health_lead_status'] == b'2018 enrolled no payment info':
                                ooo['health_lead_status_details'] = 'PAYMENT FIXED'
                            elif ooo['health_lead_status'] == b'2017 enrollment error':
                                ooo['health_lead_status_details'] = 'DIFFERENT FROM QUOTE'
                            elif ooo['health_lead_status'] == b'2015 marketplace application':
                                ooo['health_lead_status_details'] = 'PAID'
                            elif ooo['health_lead_status'] == b'2016 needs to be corrected':
                                ooo['health_lead_status_details'] = 'DIFFERENT FROM QUOTE'
                            else:
                                ooo['health_lead_status_details'] = ''
                            ooo['health_lead_status'] = 'ENROLLED'
                        elif ooo['health_lead_status'] == b'delinquency - phone # not valid'\
                        or ooo['health_lead_status'] == b'delinquency - left vm'\
                        or ooo['health_lead_status'] == b'delinquency - transferred to bcbsfl'\
                        or ooo['health_lead_status'] == b'delinquent - tempora'\
                        or ooo['health_lead_status'] == b'delinquency - cancelled'\
                        or ooo['health_lead_status'] == b'delinquency - new'\
                        or ooo['health_lead_status'] == b'delinquency - transferred to molina':
                            if ooo['health_lead_status'] == b'delinquency - phone # not valid':
                                ooo['health_lead_status_details'] = 'Phone # Not Valid'
                            elif ooo['health_lead_status'] == b'delinquency - left vm':
                                ooo['health_lead_status_details'] = 'left vm'                
                            elif ooo['health_lead_status'] == b'delinquency - transferred to bcbsfl':
                                ooo['health_lead_status_details'] = 'TRANSFERRED TO BCBS'                
                            elif ooo['health_lead_status'] == b'delinquency - cancelled':
                                ooo['health_lead_status_details'] = 'Cancelled'                
                            elif ooo['health_lead_status'] == b'delinquency - new':
                                ooo['health_lead_status_details'] = 'New'                
                            elif ooo['health_lead_status'] == b'delinquency - transferred to molina':  
                                ooo['health_lead_status_details'] = 'Transferred to Molina'
                            else:
                                ooo['health_lead_status_details'] = ''
                            ooo['health_lead_status'] = 'DELINQUENCY'
                        elif ooo['health_lead_status'] == b'2016 phone # not valid'\
                        or ooo['health_lead_status'] == b'2018 dead - phone # not valid'\
                        or ooo['health_lead_status'] == b'2017 phone # not valid'\
                        or ooo['health_lead_status'] == b'2017 dead - not interested'\
                        or ooo['health_lead_status'] == b'other state'\
                        or ooo['health_lead_status'] == b'do not call'\
                        or ooo['health_lead_status'] == b'2016 do not touch'\
                        or ooo['health_lead_status'] == b'2018 dead - has bcbs group insurance'\
                        or ooo['health_lead_status'] == b'2018 dead - duplicate'\
                        or ooo['health_lead_status'] == b'2018 z code -to be renewed'\
                        or ooo['health_lead_status'] == b'2016 dead - medicaid'\
                        or ooo['health_lead_status'] == b'2018 dead - medicaid'\
                        or ooo['health_lead_status'] == b'2018 phone # not valid'\
                        or ooo['health_lead_status'] == b'special circumstances (dnt)'\
                        or ooo['health_lead_status'] == b'2017 dead- duplicate'\
                        or ooo['health_lead_status'] == b'phone # not valid'\
                        or ooo['health_lead_status'] == b'cs not interested'\
                        or ooo['health_lead_status'] == b'2018 dead - has ambetter - florida'\
                        or ooo['health_lead_status'] == b'dead-ni has another carrier'\
                        or ooo['health_lead_status'] == b'2015 has florida blue'\
                        or ooo['health_lead_status'] == b'2018 dead - has ambetter- florida'\
                        or ooo['health_lead_status'] == b'2018 dead - medeicaid':
                            if ooo['health_lead_status'] == b'2018 dead - phone # not valid':
                                ooo['health_lead_status_details'] = 'Phone # Not Valid'
                            elif ooo['health_lead_status'] == b'2017 phone # not valid':
                                ooo['health_lead_status_details'] = 'Phone # Not Valid'
                            elif ooo['health_lead_status'] == b'2017 dead - not interested':
                                ooo['health_lead_status_details'] = 'Not Intersted'
                            elif ooo['health_lead_status'] == b'other state':
                                ooo['health_lead_status_details'] = 'other state'
                            elif ooo['health_lead_status'] == b'do not call':
                                ooo['health_lead_status_details'] = 'Not Intersted'
                            elif ooo['health_lead_status'] == b'2016 do not touch':
                                ooo['health_lead_status_details'] = 'Not Intersted'
                            elif ooo['health_lead_status'] == b'2018 dead - has bcbs group insurance':
                                ooo['health_lead_status_details'] = 'HAS BCBS GROUP INSURANCE'
                            elif ooo['health_lead_status'] == b'2018 dead - duplicate':
                                ooo['health_lead_status_details'] = 'DUPLICATE'
                            elif ooo['health_lead_status'] == b'2018 z code -to be renewed':
                                ooo['health_lead_status_details'] = 'Not Intersted'
                            elif ooo['health_lead_status'] == b'2016 dead - medicaid':
                                ooo['health_lead_status_details'] = 'Medicaid'
                            elif ooo['health_lead_status'] == b'2018 dead - medicaid':
                                ooo['health_lead_status_details'] = 'MEDICAID'
                            elif ooo['health_lead_status'] == b'2018 phone # not valid':
                                ooo['health_lead_status_details'] = 'Phone # Not Valid'
                            elif ooo['health_lead_status'] == b'special circumstances (dnt)':
                                ooo['health_lead_status_details'] = 'Not Intersted'
                            elif ooo['health_lead_status'] == b'2017 dead- duplicate':
                                ooo['health_lead_status_details'] = 'DUPLICATE'
                            elif ooo['health_lead_status'] == b'phone # not valid':
                                ooo['health_lead_status_details'] = 'Phone # Not Valid'
                            elif ooo['health_lead_status'] == b'cs not interested':
                                ooo['health_lead_status_details'] = 'NOT INTERESTED'
                            elif ooo['health_lead_status'] == b'2018 dead - has ambetter - florida':
                                ooo['health_lead_status_details'] = 'HAS AMBETTER - FLORIDA'
                            elif ooo['health_lead_status'] == b'dead-ni has another carrier':
                                ooo['health_lead_status_details'] = 'HAS AMBETTER - FLORIDA'
                            elif ooo['health_lead_status'] == b'2015 has florida blue':
                                ooo['health_lead_status_details'] = 'HAS BCBS GROUP INSURANCE'
                            elif ooo['health_lead_status'] == b'2018 dead - has ambetter- florida':
                                ooo['health_lead_status_details'] = 'HAS AMBETTER - FLORIDA'
                            elif ooo['health_lead_status'] == b'2018 dead - medeicaid':
                                ooo['health_lead_status_details'] = 'Medicaid'
                            else:
                                ooo['health_lead_status_details'] = ''
                            ooo['health_lead_status'] = 'DEAD'
                        elif ooo['health_lead_status'] == b'2018 cancelled'\
                        or ooo['health_lead_status'] == b'2017 cancelled'\
                        or ooo['health_lead_status'] == b'prior active bcbs (cancelled pre-2016)'\
                        or ooo['health_lead_status'] == b'historically cancele'\
                        or ooo['health_lead_status'] == b'cancelled-2014'\
                        or ooo['health_lead_status'] == b'2018 cancelled - error follow-up'\
                        or ooo['health_lead_status'] == b'canceled'\
                        or ooo['health_lead_status'] == b'canceled - nonpay'\
                        or ooo['health_lead_status'] == b'cancelled - 2015'\
                        or ooo['health_lead_status'] == b'2016 canceled'\
                        or ooo['health_lead_status'] == b'cancelled'\
                        or ooo['health_lead_status'] == b'prior active bcbs (cancelled 2016)'\
                        or ooo['health_lead_status'] == b'previously terminated - bcbs':
                            ooo['health_lead_status'] = 'CANCELLED'
                        elif ooo['health_lead_status'] == b'2018 callback set'\
                        or ooo['health_lead_status'] == b'2018 oe u65 callback'\
                        or ooo['health_lead_status'] == b'2017 callback set'\
                        or ooo['health_lead_status'] == b'2018 medicare oe callback'\
                        or ooo['health_lead_status'] == b'cs call back later'\
                        or ooo['health_lead_status'] == b'auto agent callback (sp/creolel)'\
                        or ooo['health_lead_status'] == b'2017 oe callback'\
                        or ooo['health_lead_status'] == b'oe callback'\
                        or ooo['health_lead_status'] == b'2019 oe callback - o65'\
                        or ooo['health_lead_status'] == b'2019 oe callback - u65':
                            ooo['health_lead_status'] = 'CALLBACK SET'
                        elif ooo['health_lead_status'] == b'2018 renewal - client changes'\
                        or ooo['health_lead_status'] == b'2017 renewal complete'\
                        or ooo['health_lead_status'] == b'2016 to be renewed'\
                        or ooo['health_lead_status'] == b'2016 active'\
                        or ooo['health_lead_status'] == b'2017 active'\
                        or ooo['health_lead_status'] == b'2018 active'\
                        or ooo['health_lead_status'] == b'active'\
                        or ooo['health_lead_status'] == b'2017 medicare active'\
                        or ooo['health_lead_status'] == b'active-paid'\
                        or ooo['health_lead_status'] == b'health - no dental':
                            if ooo['health_lead_status'] == b'2018 renewal - client changes':
                                ooo['health_lead_status_details'] = 'CLIENT CHANGES'
                            elif ooo['health_lead_status'] == b'2017 renewal complete':
                                ooo['health_lead_status_details'] = 'RENEWAL'
                            elif ooo['health_lead_status'] == b'2016 to be renewed':
                                ooo['health_lead_status_details'] = 'Pending'
                            else:
                                ooo['health_lead_status_details'] = ''
                            ooo['health_lead_status'] = 'ACTIVE'
                        elif ooo['health_lead_status'] == b'2016 dead - not interested'\
                        or ooo['health_lead_status'] == b'2018 dead - not interested'\
                        or ooo['health_lead_status'] == b'2016 recontacted'\
                        or ooo['health_lead_status'] == b'has other carrier'\
                        or ooo['health_lead_status'] == b''\
                        or ooo['health_lead_status'] == b'2018 dead - not qualified'\
                        or ooo['health_lead_status'] == b'2017 dead - medicaid'\
                        or ooo['health_lead_status'] == b'dead-not interested'\
                        or ooo['health_lead_status'] == b'2018 will not renew'\
                        or ooo['health_lead_status'] == b'2016 dead has other insurance':
                            ooo['health_lead_status'] = '2019 NEW'
                            print('si llega')
                        elif ooo['health_lead_status'] == b'2017 auto lead'\
                        or ooo['health_lead_status'] == b'2016 home lead'\
                        or ooo['health_lead_status'] == b'2016 life lead'\
                        or ooo['health_lead_status'] == b'p and c client'\
                        or ooo['health_lead_status'] == b'ancillary only'\
                        or ooo['health_lead_status'] == b'2017 application submitted-stm'\
                        or ooo['health_lead_status'] == b'2017 medicare agent callback'\
                        or ooo['health_lead_status'] == b'2017 medicare pending'\
                        or ooo['health_lead_status'] == b'medicare'\
                        or ooo['health_lead_status'] == b'cs left message'\
                        or ooo['health_lead_status'] == b'mailer sep'\
                        or ooo['health_lead_status'] == b'2017 medicare enrolled'\
                        or ooo['health_lead_status'] == b'2017 application submitted-fixed benefit'\
                        or ooo['health_lead_status'] == b'2018 presold-stm'\
                        or ooo['health_lead_status'] == b'2017 presold-fixed benefit'\
                        or ooo['health_lead_status'] == b'-- not set --'\
                        or ooo['health_lead_status'] == b'email only'\
                        or ooo['health_lead_status'] == b'2015 lead request'\
                        or ooo['health_lead_status'] == b'2016 medicare lead'\
                        or ooo['health_lead_status'] == b'2017 medicare presold'\
                        or ooo['health_lead_status'] == b'2017 requote requested':
                            ooo['health_lead_status'] = ''
                    
                    while True:
                        if(hour_in_range(start, end, datetime.datetime.now().hour)):
                            r = requests.post(url, data = json.dumps(ooo))
                            print(file_name, von_count, r.status_code, ooo['uuid'])
                            break
                        else:
                            print('wating for the right time')
                            yield gen.Task(loop.add_timeout, timedelta(minutes=15))
                    if r.status_code != 201:
                        pingRiak = subprocess.run(['riak', 'ping'], stdout=subprocess.PIPE)
                        if('pong' not in str(pingRiak.stdout)):
                            quit()
    ioloop.IOLoop.current().stop()

def main():
    print("Current time: {}".format(datetime.datetime.now().hour))
    post_csv()
    ioloop.IOLoop.current().start()

if __name__ == '__main__':
    main()
