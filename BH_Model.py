import pandas as pd
import json
import os
import pandasql as ps
import warnings
import argparse

def check_auto_include(M1730_PHQ2_LACK_INTRST,M1730_PHQ2_DPRSN,M1720_WHEN_ANXIOUS,M1745_BEH_PROB_FREQ):
    list_1 = [M1730_PHQ2_LACK_INTRST,M1730_PHQ2_DPRSN,M1720_WHEN_ANXIOUS]
    dest_list = []
    for i in list_1:
        if isinstance(i,str):
            dest_list.append(1) if i.isdigit() is True and int(i) == 3 else dest_list.append(0)
        else:
            dest_list.append(1) if int(i) == 1 else dest_list.append(0)
    M1745_BEH_PROB = M1745_BEH_PROB_FREQ
    if isinstance(M1745_BEH_PROB,str):
        dest_list.append(1) if M1745_BEH_PROB.isdigit() is True and int(M1745_BEH_PROB) in (5,6) else dest_list.append(0)
    else:
        dest_list.append(1) if int(M1745_BEH_PROB) in (5,6) else dest_list.append(0)
    return sum(dest_list)

def get_M1740(M1740_BD_IMP_DECISN,M1740_BD_VERBAL,M1740_BD_PHYSICAL,M1740_BD_SOC_INAPPRO,M1740_BD_DELUSIONS):
    source_list = [M1740_BD_IMP_DECISN,M1740_BD_VERBAL,M1740_BD_PHYSICAL,M1740_BD_SOC_INAPPRO,M1740_BD_DELUSIONS]
    dest_list = []
    for i in source_list:
        if isinstance(i,str):
            dest_list.append(int(i) if i.isdigit() is True and int(i) == 1 else 0)
        else:
            dest_list.append(i) if int(i) == 1 else dest_list.append(0)
    return sum(dest_list)

def get_icd10(M1021_PRIMARY_DIAG_ICD,M1023_OTH_DIAG1_ICD,M1023_OTH_DIAG2_ICD,M1023_OTH_DIAG3_ICD,M1023_OTH_DIAG4_ICD,M1023_OTH_DIAG5_ICD,bh_codes):
    bcd_df = bh_codes.dropna(subset=['Code'])
    li = bcd_df['Code'].tolist()
    diag_list = [M1021_PRIMARY_DIAG_ICD,M1023_OTH_DIAG1_ICD,M1023_OTH_DIAG2_ICD,M1023_OTH_DIAG3_ICD,M1023_OTH_DIAG4_ICD,M1023_OTH_DIAG5_ICD]
    dest_list = []
    for i in diag_list:
        if i in li:
            dest_list.append(1)
        else:
            dest_list.append(0)
    return sum(dest_list)


def main(path_to_input_file,path_to_bhcodes_file):
    df = pd.read_csv(path_to_input_file,delimiter = ',')
    bh_codes = pd.read_csv(path_to_bhcodes_file,delimiter = ',')
    
    my_dict = df.to_dict('records')
    score = 0
    for sub in my_dict:
        if (sub['M1710_When_confused'] == 'NA' or pd.isna(sub['M1710_When_confused'])) or \
           (sub['M1720_WHEN_ANXIOUS'] == 'NA' or pd.isna(sub['M1720_WHEN_ANXIOUS'])) or \
           (sub['M1730_PHQ2_LACK_INTRST'] == 'NA' or pd.isna(sub['M1730_PHQ2_LACK_INTRST'])) or \
           (sub['M1730_PHQ2_DPRSN'] == 'NA' or pd.isna(sub['M1730_PHQ2_DPRSN'])):
            AutoDecision="Auto-Exclude"
            break
        elif check_auto_include(sub['M1730_PHQ2_LACK_INTRST'],sub['M1730_PHQ2_DPRSN'],sub['M1720_WHEN_ANXIOUS'],sub['M1745_BEH_PROB_FREQ']) > 0:
            AutoDecision="Auto-Include"
        else:
            AutoDecision="N/A"
        if AutoDecision != "N/A":
            break
        else:
            M1033_list = [sub['M1033_HOSP_RISK_WEIGHT_LOSS'],sub['M1033_HOSP_RISK_MLTPL_HOSPZTN'],sub['M1033_HOSP_RISK_MLTPL_ED_VISIT'],sub['M1033_HOSP_RISK_COMPLIANCE'],sub['M1033_HOSP_RISK_CRNT_EXHSTN']]
            M1033_dest_list = []
            for i in M1033_list:
                if isinstance(i,str):
                    M1033_dest_list.append(int(i)) if i.isdigit() is True and int(i) == 1 else M1033_dest_list.append(0)
                else:
                    M1033_dest_list.append(i) if int(i) == 1 else M1033_dest_list.append(0)
            score = score + 1 if sum(M1033_dest_list) > 0 else score
            M1033_HOSP = sub['M1033_HOSP_RISK_MNTL_BHV_DCLN']
            if isinstance(M1033_HOSP,str):
                var = int(M1033_HOSP) if M1033_HOSP.isdigit() is True and int(M1033_HOSP) == 1 else 0
            else:
                var = M1033_HOSP if int(M1033_HOSP) == 1 else 0
            score = score + 1 if var == 1 else score
            M1720_ANX = sub['M1720_WHEN_ANXIOUS']
            if isinstance(M1720_ANX,str):
                var1 = int(M1720_ANX) if M1720_ANX.isdigit() is True and int(M1720_ANX) == 2 else 0
            else:
                var1 = M1720_ANX if int(M1720_ANX) == 2 else 0
            score = score + 1 if var1 > 0 else score
            m1740 = get_M1740(sub['M1740_BD_IMP_DECISN'],sub['M1740_BD_VERBAL'],sub['M1740_BD_PHYSICAL'],sub['M1740_BD_SOC_INAPPRO'],sub['M1740_BD_DELUSIONS'])
            score=score+m1740
            M1745_BEH = sub['M1745_BEH_PROB_FREQ']
            if isinstance(M1745_BEH,str):
                var2 = int(M1745_BEH) if M1745_BEH.isdigit() is True and int(M1745_BEH) in (3,4) else 0
            else:
                var2 = M1745_BEH if int(M1745_BEH) in (3,4) else 0
            score = score + 1 if var2 == 1 else score
            score = score +1
            M1730_list = [sub['M1730_PHQ2_LACK_INTRST'],sub['M1730_PHQ2_DPRSN']]
            M1730_dest_list = []
            for i in M1730_list:
                if isinstance(i,str):
                    M1730_dest_list.append(int(i)) if i.isdigit() is True and int(i) == 2 else M1730_dest_list.append(0)
                else:
                    M1730_dest_list.append(i) if int(i) == 2 else M1730_dest_list.append(0)
            score = score + 1 if sum(M1730_dest_list) > 0 else score
            icd10 = get_icd10(sub['M1021_PRIMARY_DIAG_ICD'],sub['M1023_OTH_DIAG1_ICD'],sub['M1023_OTH_DIAG2_ICD'],sub['M1023_OTH_DIAG3_ICD'],sub['M1023_OTH_DIAG4_ICD'],sub['M1023_OTH_DIAG5_ICD'],bh_codes)
            score = score+icd10
            result = "Include" if score>=3 else "Exclude"
            print("AutoDecision : "+AutoDecision)
            print("Result : "+result)
            print("Score : "+str(score))
    if AutoDecision == 'NA':
        print("AutoDecision : "+AutoDecision)

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument('-i','--input', help = "Location of Input File", required=False, default='E:\BH_Trigger\BH_Input_Final.csv')
    parser.add_argument('-b','--bhcodes', help = "Location of BH Codes File", required = False, default = 'E:\BH_Trigger\BH_ICD10_Codes.csv')
    args = parser.parse_args()
    main(args.input,args.bhcodes)
