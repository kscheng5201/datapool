from datetime import timedelta, datetime
from pathlib import Path
import pandas as pd 
import os 
import pymysql


def DbConnRead_df(sql, db_name):
    """
    Description:
    給資料庫名字以及sql敘述就回傳指定的表單(df)
    Args:
    sql: (str) sql敘述語句
    db_name: (str) 資料庫名字，詳參閱 configs.db_configs 說明
    
    """
    db_dicts= {}

    db_dicts["cdp"]={
        "host":"accu-qa-11-29.mysql.database.azure.com",
        "port":3306,
        "user":"benson_analytic@accu-qa-11-29",
        "password":"!##Ana@lytic!!"
    }

    db_dicts["datapool_dev"]={
        "host":"accu-data-science-rds-etl-datapool-prd-01.cfilkryz7gav.ap-northeast-1.rds.amazonaws.com",
        "port":3306,
        "user":"3dm_worker",
        "password":"BvM4se3BXP"
    }

    db_settings = db_dicts[db_name]
    
    try:
        conn = pymysql.connect(**db_settings)
    except Exception as ex:
        print(ex)

    df = pd.read_sql(sql,conn)

    return df




def Dashborad_Update(start_date:str=None,before_days=0):
    """
    start_date: 'YYYY-MM-DD' (eg. '2021-12-09')
    """
    today_dt = datetime.today() - timedelta(days=1)
    today = today_dt.strftime('%Y-%m-%d')
    if start_date is None:
        start = today_dt- timedelta(days=before_days)
        start_date = start.strftime('%Y-%m-%d')
    # datetime.datetime.strptime('2021-11-29',"%Y-%m-%d")
    else:
        start = datetime.strptime(start_date,"%Y-%m-%d")
        start = start_date - timedelta(days=before_days)
        start_date = start.strftime('%Y-%m-%d')
    day_range = pd.date_range(start_date, today, freq='D').format(formatter=lambda x: x.strftime('%Y-%m-%d'))

    return day_range

def input_dirpath(FileFolderDir:str=None, products=['nix','cdp'],indicate=False):
    if FileFolderDir is None:
        FileDir = os.path.realpath(__file__) #當前路徑
        FileFolderDir = os.path.normpath(FileDir + os.sep + os.pardir) #前一層路徑
    RootDir = os.path.normpath(FileFolderDir + os.sep + os.pardir)

    path_dict =  {}
    
    path_dict['dir_root']=RootDir
    path_dict['dir_file_folder']=FileFolderDir
    DataPooldir = RootDir +"/datapool_folder"
    path_dict['dir_datapool_folder']= DataPooldir

    for i in ['export_file','error_log']:
        path_dict[f'dir_{i}'] = DataPooldir +f'/{i}'
        for j in products:
            tmp = path_dict[f'dir_{i}']+ f'/{j}'
            path_dict[f'{i}_{j}']= tmp
            tmp = Path(tmp)
            tmp.mkdir(parents=True, exist_ok=True)
            del tmp
    if indicate is True:    
        print("執行程式當前路徑為", FileFolderDir, )
        print("將以路徑", RootDir, "為 root 於其建立如下結構：" )
    desc ="""
        從當前路徑（file_folder）的前一層開始，相關資料夾將自動以如下路徑結構生成：
            root- file_folder

                - datapool_folder - export_file   - nix 
                                                  - cdp

                                  - error_log     - nix 
                                                  - cdp

        並回傳 dict. 提供路徑字串選擇類別如下：
        1. ['dir_root']
        2. ['dir_file_folder','dir_datapool_folder']
        3. ['dir_export_file, 'dir_error_log']
        4. ['export_file_nix', 'export_file_cdp', 'error_log_nix', 'error_cdp']
        """
    if indicate is True:
        print(desc)
    return path_dict


dict_Path = input_dirpath()

from datetime import datetime, timedelta
import os 

class TriggerETL:
    def __init__(self, product, org_id=None, db_id=None, from_db_type=None, to_db_type=None):

        self.product = product #['nix', 'cdp']
        self.today = datetime.today().strftime('%Y-%m-%d')  # 啟動更新當下日期
        self.tag_day = None     #統計日前一天（因為統計日當天不列入計算）
        self.final_day = None   #統計日前一天（因為統計日當天不列入計算）
        self.org_id = org_id
        self.db_id = db_id
        self.to_db_type = to_db_type # 輸出資料庫為誰？['datapool','prod','formal']，此處datapool就是測試機
        self.from_db_type = from_db_type # 從資料庫輸入的為誰？['datapool','prod','formal']，此處datapool就是測試機

    def MapPath(self,map_dict:dict=None):
        #TODO: 這樣寫太醜而且沒有彈性，應該尋找類似.update()的寫法
        map_dict = input_dirpath() if map_dict is None else map_dict
        for key in map_dict:
            setattr(self, key, map_dict[key])
        # self.dir_root = map_dict['dir_root']
        # self.dir_file_folder=map_dict['dir_file_folder']
        # self.dir_datapool_folder = map_dict['dir_datapool_folder']
        # self.dir_export_file = map_dict['dir_export_file']
        # self.export_file_nix = map_dict['export_file_nix']
        # self.export_file_cdp = map_dict['export_file_cdp']
        # self.dir_error_log = map_dict['dir_error_log']
        # self.error_log_nix = map_dict['error_log_nix']
        # self.error_log_cdp = map_dict['error_log_cdp']


    def DateUpdate(self, tag_day:str=None, org_id:int=None, db_id:int=None,file_dir:str=None, programe="shell",from_db_type=None,to_db_type=None):
        """
        日更新送到資料庫上的etl，可以提供直接給執行的.sh或是.py檔案，若不給則自動考慮找尋目錄裡面預設.sh檔
        """
        # if file_dir is None:
        self.tag_day = self.today if tag_day is None else tag_day            
        final_date = datetime.strptime(self.tag_day, "%Y-%m-%d") - timedelta(days=1)
        self.final_day= final_date.strftime('%Y-%m-%d')
        
        if db_id is not None:
            self.db_id = db_id 
        if org_id is not None:
            self.org_id = org_id
        
        if programe=="shell":
            if self.product=='nix':            
                error_path = self.error_log_nix
                export_path = self.export_file_nix
                file_dir = self.dir_file_folder + "/nix_trigger_p1.sh" if file_dir==None else file_dir
                ### 格式： {sh檔路徑}{參數1:從何種資料庫輸入}{參數2:輸出何種資料庫}{參數3:統計日}{參數4:廠商db}{參數5:輸出資料路徑}{參數6:輸出錯誤目錄路徑}
                os.system('sh ' f'{file_dir} {self.from_db_type} {self.to_db_type} {self.tag_day} {self.db_id} {export_path} {error_path}')  #抽資料出來
                
            elif self.product=='cdp':
                error_path = self.error_log_cdp
                export_path = self.export_file_cdp
                file_dir = self.dir_file_folder + "/cdp_trigger_p1.sh" if file_dir==None else file_dir
                ### 格式： {sh檔路徑}{參數1:從何種資料庫輸入}{參數2:從何種資料庫輸出}{參數3:統計日}{參數4:廠商org_id}{參數5:輸出資料路徑}{參數6:輸出錯誤目錄路徑}
                os.system('sh ' f'{file_dir} {self.from_db_type} {self.to_db_type} {self.tag_day} {self.org_id} {export_path} {error_path}')  #抽資料出來
            else:
                raise ValueError('缺少產品參數（product = nix or cdp ）')

        elif programe=="python":
            # os.system('sh ' f'AccuHit/test/nix_trigger.sh {dd}')  
            pass
  
    def DateCheck(self, tag_date:str=None):
        """
        先上資料庫檢查有沒有做過日的etl更新，注意這邊要考慮兩種，第一種是該日本來沒資料，第二是有資料但沒有更新到
        """
        # TODO: 這邊是用於確認某一天是否有被執行過
        pass

    def PeriodCheck(self, periods=90):
        """
        確認今天往前看 period (90天) 內資料是否有缺漏，但新客戶加入不滿 period (90天) 是例外。
        """
        #TODO:回傳有缺少的日期
        pass

class TaggablesMethod:
    def __init__(self, product=None, org_id=None, db_id=None,db_type=None,update_way=None):
        self.product=product
        self.org_id=org_id
        self.db_id=db_id
        self.db_type=db_type
        self.update_way=update_way

    def MapPath(self,map_dict:dict=None):
        #TODO: 這樣寫太醜而且沒有彈性，應該尋找類似.update()的寫法
        map_dict = input_dirpath() if map_dict is None else map_dict
        for key in map_dict:
            setattr(self, key, map_dict[key])
        # self.dir_root = map_dict['dir_root']
        # self.dir_file_folder=map_dict['dir_file_folder']
        # self.dir_datapool_folder = map_dict['dir_datapool_folder']
        # self.dir_export_file = map_dict['dir_export_file']
        # self.export_file_nix = map_dict['export_file_nix']
        # self.export_file_cdp = map_dict['export_file_cdp']
        # self.dir_error_log = map_dict['dir_error_log']
        # self.error_log_nix = map_dict['error_log_nix']
        # self.error_log_cdp = map_dict['error_log_cdp']

    def TriggerTaggables(self, dir_file,tag_day=None):
        if tag_day is None:
            tag_day = (datetime.today().strftime('%Y-%m-%d')- timedelta(days=1)).strftime('%Y-%m-%d')
        if self.product=='nix':
            ### 格式： {sh檔路徑}{參數1:從何處輸入並輸出資料庫}{參數2:統計日}{參數3:廠商db}{參數4:error log 輸出路徑}
            os.system('sh ' f'{dir_file} {self.db_type} {tag_day} {self.db_id} {self.error_log_nix}')  
        elif self.product=='cdp':
            ### 格式： {sh檔路徑}{參數1:從何處輸入並輸出資料庫}{參數2:統計日}{參數3:廠商db}{參數4:error log 輸出路徑}
            os.system('sh ' f'{dir_file} {self.db_type} {tag_day} {self.org_id} {self.error_log_cdp} {self.update_way}')  
            
            

if __name__ == '__main__':
    update_way='first' #'daily'
    day_range = Dashborad_Update(before_days=90)
    dict_Path = input_dirpath()
    
    sql="select org_id from cdp_organization.organization_domain where domain_type='web' group by org_id"
    df = DbConnRead_df(sql=sql, db_name='cdp')

    for org_id in [3,4,6]:#df.org_id:
        """先針對指定的org_id列出所有所屬的db_id存成txt"""
        sql=f"select db_id from cdp_organization.organization_domain where org_id={org_id} and domain_type='web'"
        df_db_id = DbConnRead_df(sql=sql, db_name='cdp')
        export_dir = dict_Path['export_file_cdp']
        df_db_id.to_csv(f'{export_dir}/trigger_{org_id}_db_id.txt', header=False, index=False)
        
        # """先對天更新"""
        for dd in day_range:
            clss_TriggerEtl = TriggerETL(product='cdp',from_db_type='cdp',to_db_type='datapool') #輸出到datapool
            clss_TriggerEtl.MapPath(map_dict=dict_Path) #建立路徑
            clss_TriggerEtl.DateUpdate(tag_day=dd, org_id=org_id)
            print(dd)

    for org_id in [3,4,6]:
        """製作90天內的 taggables"""
        class_TriggerTaggables= TaggablesMethod(product='cdp', org_id=org_id, db_type='datapool', update_way=update_way)
        class_TriggerTaggables.MapPath(map_dict=dict_Path)
        dir_file = class_TriggerTaggables.dir_file_folder + "/cdp_trigger_p2.sh"
        class_TriggerTaggables.TriggerTaggables(dir_file=dir_file, tag_day=dd)
    
        """製作儀表板"""
        dir_file = dict_Path['dir_file_folder'] + "/cdp_trigger_p3.sh"
        db_type='datapool'
        error_log = dict_Path['error_log_cdp']
        os.system('sh ' f'{dir_file} {db_type} {dd} {org_id} {error_log}') 
        # breakpoint()
        