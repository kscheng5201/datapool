
from os import error
from numpy.testing._private.utils import raises
from datetime import datetime, timedelta,timezone
tz = timezone(timedelta(hours=+8))


class LogRepo:
    def __init__(self,TELE_TOKEN=None,TELE_CHATID=None,JANDI_URL=None):
        """
        若是有用到telegram就需要給參數 TELE_TOKEN, TELE_CHATID，另外若是有用到jandi就需要給參數 JANDI_URL
        Args:
            TELE_TOKEN: 即來自telegram機器人的toekn
            TELE_CHATID: 即來自telegram群組的chat_idNone
            JANDI_URL: Jandi的URL
        Return:
            若是對應的參數有提供則class裡面會各自紀錄在 channel_key= 'tele_public' 以及 'jandi_public'，即是，
            之後可以直接利用 channel_key 決定訊息送至指定處，詳請參閱func.:'send' , 'Tele_send' 或是 'Jandi_send' 
        """
        self.TELE_TOKEN=TELE_TOKEN
        chat_id = -753082774 if TELE_CHATID is None else TELE_CHATID

        self.tele_public={
            'TELE_TOKEN': TELE_TOKEN,
            'chat_id': chat_id
            }
        self.jandi_public={
            'url':JANDI_URL
        }
    
    def __getattr__(self, item):
        return self.__dict__[item]

    def words_limit(self, text, str_limit):
        if text is None:
            return text
        if len(text)>str_limit:
            limit = int(str_limit/2)
            begin = text[:limit]
            end = text[-limit:]
            words = begin +"... \n . \n . \n . \n . \n . \n ..."+ end
        else:
            words = text
        return words
    def config_format(self,message_type):
        config = {
            'info':{'connectColor':'#48fa1b','title_type':'INFO'},          #INFO是綠色
            '上傳成功':{'connectColor':'#48fa1b','title_type':'上傳成功'},          #INFO是綠色
            
            'warning':{'connectColor':'#FAC11B','title_type':'WARNING'},    #WARNING是黃色
            'error':{'connectColor':'#ff0000','title_type':'ERROR'},        #ERROR是紅色
            
            '程式錯誤':{'connectColor':'#ff0000','title_type':'程式錯誤'},        #ERROR是紅色
            
            'debug':{'connectColor':'#1500ff','title_type':'DEBUG'},        #DEBUG是藍色
            }
        return config[message_type]

#================================================================================================================

    def Tele_add(self,channel_key,chat_id,token=None):
        """
        新增聊天群組，使用此功能之前需要確認該機器人已經被加入該群組
        Args:
            channel_key: str, 群組名稱
            chat_id: int, 即來自telegram群組的chat_id
            token: str,  即來自telegram機器人的toekn
        Notic: 
            如果之前在
        """
        if channel_key!='TELE_TOKEN':
            token = self.TELE_TOKEN if token is None else token
            if token is None:
                raise ValueError(f"function Tele_add is lack of argument: token.")
            content = {
                'TELE_TOKEN': token,
                'chat_id':chat_id
                }
            setattr(self, channel_key, content)
        else:
            raise NameError('Key cannot be TELE_TOKEN, please change.')

    def Jandi_add(self, channel_key, url):
        """
        新增聊天群組，使用此功能之前需要確認該機器人已經被加入該群組
        Args:
            channel_key: str, 群組名稱
            url: str,  即來自jandi 群組的 url
        
        """
        
        content = {
            'url': url
            }
        setattr(self, channel_key, content)        
        
    def Tele_send(
            self,
            brief=None, 
            detail=None,
            channel_key='tele_public',
            message_type='info',
            tele_token=None,
            tele_chatID=None,
            words_limit=None
            ):
        
        """
        送訊息至指定聊天群組，預設是送至公共群組，若有提供tele_token以及tele_chatID，則會送指定的聊天群組

        Args:
            brief: str, 訊息摘要內容
            detail: str, 訊息細節內容
            channel_key: str, 欲送至的群組，預設是公共群組(3DMReport)
            message_type: str, enumerat ['info','warning','error','debug']，各自訊息代表類型通知、警示、錯誤以及除錯模式
            tele_token: str, telegram機器人的token
            tele_chatID: int, telegram聊天群組的chat_id
            words_limit: int, 字數限制，僅保留字串頭尾段限制的字數，預設是不限制字數
        """

        import requests
        from datetime import datetime
        dct=self.__getattr__(channel_key)

        if (dct is None) and (tele_token is None):
            raise ValueError(f"function Tele_send is lack of argument: tele_token. Please check both arguments:tele_token and tele_chatID given.")
        elif (dct is None) and (tele_chatID is None):
            raise ValueError(f"function Tele_send is lack of argument: tele_chatID.Please check both arguments:tele_token and tele_chatID given.")
        

        TELE_TOKEN=dct['TELE_TOKEN'] if tele_token is None else tele_token
        chat_id =dct['chat_id'] if tele_chatID is None else tele_chatID
        now = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
        config = self.config_format(message_type)
        title_type = config['title_type']

        brief = self.words_limit(brief, str_limit=words_limit) if words_limit else brief
        detail = self.words_limit(detail, str_limit=words_limit) if words_limit else detail
        brief = '' if brief is None else brief
        detail = '' if detail is None else detail
        final_text = f'[{now}]' + "\n" + title_type + "\n"+ brief +"\n"+ "Detail" + "\n" + detail
        URL = "https://api.telegram.org/bot{}/".format(TELE_TOKEN)
        url = URL + "sendMessage?text={}&chat_id={}".format(final_text, chat_id)
        jj=requests.post(url)   
        
            
    def Jandi_send(
            self,
            brief=None, 
            detail=None,
            channel_key='jandi_public',
            message_type='info',
            jandi_url=None,
            words_limit=None
            ):        
        """
        送訊息至指定聊天群組，預設是送至公共群組，若有提供tele_token以及tele_chatID，則會送指定的聊天群組
        Args:
            brief: str, 訊息摘要內容
            detail: str, 訊息細節內容
            channel_key: str, 欲送至的群組，若創建 class 有提供 JANDI_URL，則預設值為 'jandi_public'
            message_type: str, enumerat ['info','warning','error','debug']，各自訊息代表類型通知、警示、錯誤以及除錯模式
            jandi_url: str, Jandi的URL
            words_limit: int, 字數限制，僅保留字串頭尾段限制的字數，預設是不限制字數


        補充：原curl的內容應是如下
        curl \
            -X POST https://wh.jandi.com/connect-api/webhook/24388692/c4fbe553675caf6092dcbf0bce0ece2d \
            -H "Accept: application/vnd.tosslab.jandi-v2+json" \
            -H "Content-Type: application/json" \
            --data-binary '{"body":"[[PizzaHouse]](http://url_to_text) You have a new Pizza order.","connectColor":"#FAC11B","connectInfo":[{"title":"Topping","description":"Pepperoni"},
            {"title":"Location","description":"Empire State Building, 5th Ave, New York","imageUrl":"Url_to_text"}]}'
        
        而將 CURL轉換成python格式可以直接參照 https://reqbin.com/req/python/c-d2nzjn3z/curl-post-body ，複製貼上就好，你會感到很快樂
        """
        import requests
        from requests.structures import CaseInsensitiveDict
        from datetime import datetime
        import json
        dct=self.__getattr__(channel_key)
        
        url=dct['url'] if jandi_url is None else jandi_url
        if url is None:
            raise ValueError(f"function Jandi_send is lack of argument: jandi_url.")
   
        headers = CaseInsensitiveDict() #不會對大小寫有分別的dict格式
        headers["Accept"] = "application/vnd.tosslab.jandi-v2+json"
        headers["Content-Type"] = "application/json"



        config = self.config_format(message_type)
        connectColor=config['connectColor']
        title_type = config['title_type']

        data = """
        {"body":"","connectColor":"","connectInfo":[{"title":"","description":""}]}
        """
        now = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')

        data = json.loads(data)
        data['body']=now
        data['connectColor']=connectColor

        block = data['connectInfo']         
        block[0]['title']=title_type
        block[0]['description']=brief
        if detail is not None:
            detail = self.words_limit(detail, words_limit) if words_limit is not None else detail
            content={"title":"Detail","description":f"{detail}"}
            block.append(content)
            data['connectInfo']=block

        data = json.dumps(data)
        
        resp = requests.post(url, headers=headers, data=data.encode()) 
                
    def send(
            self,
            channel_key=None,
            brief=None, 
            detail=None,
            message_type='info',
            words_limit=None,
            bot='jandi'
            ):
        """
        這邊只看過去已新增的 channel_key 來決定送至何處
        Args:
            channel_key: str, 欲送至的群組，若 bot='jandi'，則預設值為 'jandi_public',反之若 bot='telegram'，則預設值為 'telegram_public'
            brief: str, 訊息摘要內容
            detail: str, 訊息細節內容
            message_type: str, enumerat ['info','warning','error','debug']，各自訊息代表類型通知、警示、錯誤以及除錯模式,
            words_limit: 送出訊息字數限制,
            bot: enumerat ['jandi','telegram']
        """

        if bot=='jandi':
            channel_key = 'jandi_public' if channel_key is None else channel_key
            if self.jandi_public is None:
                raise ValueError(f"function send with bot={bot} is lack of a argument: channel_key and default value jandi_public is not usable.")
            self.Jandi_send(brief=brief, detail=detail,channel_key=channel_key,message_type=message_type,words_limit=words_limit)

        elif bot=='telegram':
            channel_key = 'tele_public' if channel_key is None else channel_key
            if self.tele_public is None:
                raise ValueError(f"function send with bot={bot} is lack of a argument: channel_key and default value tele_public is not usable.")
            self.Tele_send(brief=brief, detail=detail, channel_key=channel_key, message_type=message_type,words_limit=words_limit)
            
    def add(
            self,
            channel_key,
            jandi_url=None,
            tele_chatID=None,
            tele_token=None,
            bot='jandi'
            ):
        """
        新增聊天群組，使用此功能之前需要確認選擇何種類型的機器人以及該機器人是否已經被加入該群組
        Args:
            channel_key: str, 群組名稱
            jandi_url: str,  即來自jandi 群組的 url，選擇jandi則須提供此項
            tele_chatID: str,  即來自telegram 群組的chat_id，選擇telegram則須提供此項
            tele_token: int,  即來自telegram 群組的token，選擇telegram則須提供此項
            bot: enumerat ['jandi','telegram']，預設是 'jandi'
        """

        if bot=='jandi':
            if jandi_url is None:
                raise ValueError(f"function add with bot={bot} is lack of argument: jandi_url.")
            self.Jandi_add(channel_key=channel_key, url=jandi_url)
        elif bot=='telegram':
            if tele_chatID is None:
                raise ValueError(f"function add with bot={bot} is lack of argument: tele_chatID")
            if (self.TELE_TOKEN is None) and (tele_token is None):
                raise ValueError(f"function add with bot={bot} is lack of argument: tele_token, because you don't give argument of TELE_TOKEN at the beginning." )
            tele_token = self.TELE_TOKEN if tele_token is None else tele_token
            self.Tele_add(channel_key=channel_key,chat_id=tele_chatID,token=tele_token) 

    def admin_show():
        """內存已經有的名單
        """
        pass
