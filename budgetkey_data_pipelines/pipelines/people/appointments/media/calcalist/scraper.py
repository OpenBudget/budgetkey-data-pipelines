import logging

from selenium import webdriver
from datapackage_pipelines.utilities.resources import PROP_STREAMING
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from urllib.parse import urljoin
from pyquery import PyQuery as pq
from init_nominations_page import InitNominationsPage
from nominations_page import NominationsPage

from datapackage_pipelines.wrapper import ingest, spew

parameters, datapackage, _ = ingest()

CALCALIST_BASE_URL = 'http://www.calcalist.co.il'


def scrape():
    logging.info('Scraping calcalist')

    driver = webdriver.PhantomJS()
    driver.set_window_size(1200, 800)

    logging.info('Selenium driver initialized')

    full_init_url = urljoin(CALCALIST_BASE_URL, '/local/home/0,7340,L-3789,00.html')
    logging.info('Initial URL: %s' % full_init_url)
    driver.get(full_init_url)
    init_page = InitNominationsPage(CALCALIST_BASE_URL, driver.page_source)
    url = init_page.nominations_url()
    logging.info('IFrame URL: %s' % url)

    i = 0
    while url is not None and i < 3:
        logging.info('iteration #%d with url: %s' % (i, url))
        driver.get(url)
        nominations_page = NominationsPage(url, CALCALIST_BASE_URL, driver.page_source)

        for nomination in nominations_page.nominations():
            nomination['source'] = 'calcalist'
            yield nomination

        url = nominations_page.next_url()
        logging.info('next URL: %s' % url)
        i += 1


def test_html():
    return """
   <BODY style='margin:0px;' bgcolor=#e9e8e3 onload='goSetHeight()'>
   <a name='topnomination'></a>
   <div class=MainArea>
      <div class=MainArea_Inner>
         <table cellspacing='0' cellpadding='0'>
            <tr>
               <td align=right valign=top>
                  <div class=Nom_Date>15.11.2016</div>
                  <div class=Nom_Title><a href='javascript:;'  style='cursor:default' target=_parent>ירון יצהרי</a></div>
                  <div class=Nom_Comp>מדטרוניק</div>
                  <div class=Nom_SubTitle><a href='javascript:;'  style='cursor:default' target=_parent >ירון יצהרי מונה למנכ"ל חברת המיכשור הרפואי מדטרוניק בישראל. קודם היה סמנכ"ל ומנהל הפתוח העסקי של החברה</a></div>
               </td>
               <td width=10></td>
               <td width=130 align=right valign=top><a href='javascript:;'  style='cursor:default' target=_parent ><img src='https://images1.calcalist.co.il/PicServer3/2016/11/15/664573/14_s.jpg' border=0  alt='צילום: רמי זרנגר'  title='צילום: רמי זרנגר'   width=128 height=88 HM=1></a></td>
            </tr>
         </table>
         <div class=sep></div>
         <table cellspacing='0' cellpadding='0'>
            <tr>
               <td align=right valign=top>
                  <div class=Nom_Date>13.11.2016</div>
                  <div class=Nom_Title><a href='javascript:;'  style='cursor:default' target=_parent>גיל רשף</a></div>
                  <div class=Nom_Comp>המכללה האקדמית נתניה</div>
                  <div class=Nom_SubTitle><a href='javascript:;'  style='cursor:default' target=_parent >גיל רשף מונה למנכ"ל המכללה האקדמית נתניה. קודם כיהן בשורת תפקידי ניהול בכירים במוסדות אקדמיים, בהם: מנכ"ל המכללה האקדמית אונו, מנכ"ל המרכז ללימודים אקדמיים אור יהודה ומנכ"ל ומייסד ועד המכללות הלא מתוקצבות</a></div>
               </td>
               <td width=10></td>
               <td width=130 align=right valign=top><a href='javascript:;'  style='cursor:default' target=_parent ><img src='https://images1.calcalist.co.il/PicServer3/2016/11/13/663877/15_s.jpg' border=0  alt=''  title=''   width=128 height=88 HM=1></a></td>
            </tr>
         </table>
         <div class=sep></div>
         <table cellspacing='0' cellpadding='0'>
            <tr>
               <td align=right valign=top>
                  <div class=Nom_Date>13.11.2016</div>
                  <div class=Nom_Title><a href='javascript:;'  style='cursor:default' target=_parent>פרופ' שוש ארד</a></div>
                  <div class=Nom_Comp>המכללה האקדמית אחוה</div>
                  <div class=Nom_SubTitle><a href='javascript:;'  style='cursor:default' target=_parent >פרופ' שוש ארד מונתה לנשיאת המכללה האקדמית אחוה. קודם היתה נשיאת המרכז האקדמי רופין</a></div>
               </td>
               <td width=10></td>
               <td width=130 align=right valign=top><a href='javascript:;'  style='cursor:default' target=_parent ><img src='https://images1.calcalist.co.il/PicServer3/2016/11/13/663875/14_S.jpg' border=0  alt='צילום: יח&quot;צ'  title='צילום: יח&quot;צ'   width=128 height=88 HM=1></a></td>
            </tr>
         </table>
         <div class=sep></div>
         <table cellspacing='0' cellpadding='0'>
            <tr>
               <td align=right valign=top>
                  <div class=Nom_Date>13.11.2016</div>
                  <div class=Nom_Title><a href='javascript:;'  style='cursor:default' target=_parent>דניאל לבנטל</a></div>
                  <div class=Nom_Comp>מישורים</div>
                  <div class=Nom_SubTitle><a href='javascript:;'  style='cursor:default' target=_parent >דניאל לבנטל מונה למנכ"ל חברת הנדל"ן מישורים. בתפקידו האחרון שימש כמשנה למנכ"ל קבוצת בסט</a></div>
               </td>
               <td width=10></td>
               <td width=130 align=right valign=top><a href='javascript:;'  style='cursor:default' target=_parent ><img src='https://images1.calcalist.co.il/PicServer3/2016/11/13/663870/555_s.jpg' border=0  alt=''  title=''   width=128 height=88 HM=1></a></td>
            </tr>
         </table>
         <div class=sep></div>
         <table cellspacing='0' cellpadding='0'>
            <tr>
               <td align=right valign=top>
                  <div class=Nom_Date>13.11.2016</div>
                  <div class=Nom_Title><a href='javascript:;'  style='cursor:default' target=_parent>רונית רובין</a></div>
                  <div class=Nom_Comp>אולקלאוד</div>
                  <div class=Nom_SubTitle><a href='javascript:;'  style='cursor:default' target=_parent >רונית רובין מונתה למנכ"לית לחברת אולקלאוד. קודם היתה היתה חברת הנהלה וסמנכ"ל החטיבה העסקית בפרטנר</a></div>
               </td>
               <td width=10></td>
               <td width=130 align=right valign=top><a href='javascript:;'  style='cursor:default' target=_parent ><img src='https://images1.calcalist.co.il/PicServer3/2016/11/13/663871/13.jpg' border=0  alt=''  title=''   width=128 height=88 HM=1></a></td>
            </tr>
         </table>
         <div class=sep></div>
         <table cellspacing='0' cellpadding='0'>
            <tr>
               <td align=right valign=top>
                  <div class=Nom_Date>13.11.2016</div>
                  <div class=Nom_Title><a href='javascript:;'  style='cursor:default' target=_parent>אייל ברש</a></div>
                  <div class=Nom_Comp>ArcServe </div>
                  <div class=Nom_SubTitle><a href='javascript:;'  style='cursor:default' target=_parent >אייל ברש (43), מונה למנהל פעילות ArcServe בישראל. בתפקדיו הקודמים שימש כמנהל שיווק אזורי באפריקה ואגן הים התיכון בנט-אפ (NettApp), דירקטור ערוצים ושותפים ב-CA  ישראל, מנהל פיתוח עסקי ב-EMC באזור איבריה, שוויץ, ישראל ויוון, ומנהל פעילות פיתוח עסקי בחברת Musketeer ובמתחם היזמות OpenValley ברמת-ישי</a></div>
               </td>
               <td width=10></td>
               <td width=130 align=right valign=top><a href='javascript:;'  style='cursor:default' target=_parent ><img src='https://images1.calcalist.co.il/PicServer3/2016/11/13/663825/123_s.jpg' border=0  alt='צילום: קובי קנטור'  title='צילום: קובי קנטור'   width=128 height=88 HM=1></a></td>
            </tr>
         </table>
         <div class=sep></div>
         <table cellspacing='0' cellpadding='0'>
            <tr>
               <td align=right valign=top>
                  <div class=Nom_Date></div>
                  <div class=Nom_Title><a href='javascript:;'  style='cursor:default' target=_parent>שלומית פן</a></div>
                  <div class=Nom_Comp>לפידות</div>
                  <div class=Nom_SubTitle><a href='javascript:;'  style='cursor:default' target=_parent >שלומית פן, מונתה לדירקטורית בחברת לפידות, שבבעלות יעקב לוקסמבורג. פן,  בעלת תואר ראשון בכלכלה ומדעי המדינה מאונ' תל אביב ותואר שני MBA מאוניברסיטת ברדפורד, מכהנת בין היתר כדח''צית בחברות וויליפוד השקעות ואולטרה אקוויטי.</a></div>
               </td>
               <td width=10></td>
               <td width=130 align=right valign=top><a href='javascript:;'  style='cursor:default' target=_parent ><img src='https://images1.calcalist.co.il/PicServer2/20122005/398441/aaa_S.jpg' border=0  alt=''  title=''   width=128 height=88 HM=1></a></td>
            </tr>
         </table>
         <div class=sep></div>
         <table cellspacing='0' cellpadding='0'>
            <tr>
               <td align=right valign=top>
                  <div class=Nom_Date>06.11.2016</div>
                  <div class=Nom_Title><a href='javascript:;'  style='cursor:default' target=_parent>עדי נאה גמליאל </a></div>
                  <div class=Nom_Comp>2BSecure</div>
                  <div class=Nom_SubTitle><a href='javascript:;'  style='cursor:default' target=_parent >עדי נאה גמליאל (37) מונה ל-CTO ב- 2BSecure, חברת אבטחת המידע והסייבר של מטריקס. בין תפקידיו האחרונים שימש נאה גמליאל כמנהל ארכיטקטורה וחדשנות באורבוטק</a></div>
               </td>
               <td width=10></td>
               <td width=130 align=right valign=top><a href='javascript:;'  style='cursor:default' target=_parent ><img src='https://images1.calcalist.co.il/PicServer3/2016/11/06/661891/3_s.jpg' border=0  alt=''  title=''   width=128 height=88 HM=1></a></td>
            </tr>
         </table>
         <div class=sep></div>
         <table cellspacing='0' cellpadding='0'>
            <tr>
               <td align=right valign=top>
                  <div class=Nom_Date>02.11.2016</div>
                  <div class=Nom_Title><a href='javascript:;'  style='cursor:default' target=_parent>מנשה דלל</a></div>
                  <div class=Nom_Comp>ברוקרים</div>
                  <div class=Nom_SubTitle><a href='javascript:;'  style='cursor:default' target=_parent >מנשה דלל מונה למנכ"ל חברת "ברוקרים". בתפקידו האחרון שימש כמנכ"ל רשת התיווך והזכיינות הבינלאומית בישראל, Realty Executives. בעברו כיהן גם כמנהל הנדל"ן והעסקים בקבוצת פז נפט</a></div>
               </td>
               <td width=10></td>
               <td width=130 align=right valign=top><a href='javascript:;'  style='cursor:default' target=_parent ><img src='https://images1.calcalist.co.il/PicServer3/2016/11/02/661285/1_s.jpg' border=0  alt=''  title=''   width=128 height=88 HM=1></a></td>
            </tr>
         </table>
         <div class=sep></div>
         <table cellspacing='0' cellpadding='0'>
            <tr>
               <td align=right valign=top>
                  <div class=Nom_Date>01.11.2016</div>
                  <div class=Nom_Title><a href='javascript:;'  style='cursor:default' target=_parent>שלי שמיר-קינן</a></div>
                  <div class=Nom_Comp>החברה המרכזית לייצור משקאות קלים</div>
                  <div class=Nom_SubTitle><a href='javascript:;'  style='cursor:default' target=_parent >שלי שמיר-קינן מונתה לסמנכ"לית השיווק של קוקה-קולה ישראל מקבוצת החברה המרכזית לייצור משקאות קלים. קודם היתה סמנכ"לית השיווק של חברת נביעות</a></div>
               </td>
               <td width=10></td>
               <td width=130 align=right valign=top><a href='javascript:;'  style='cursor:default' target=_parent ><img src='https://images1.calcalist.co.il/PicServer3/2016/11/01/660823/4_s.jpg' border=0  alt='צילום: שוקה כהן'  title='צילום: שוקה כהן'   width=128 height=88 HM=1></a></td>
            </tr>
         </table>
         <div class=sep></div>
         <style>
            .Comments_Pagging {	font-family:'Arial','Arial (Hebrew)','David (Hebrew)','Courier New (Hebrew)';
            font-size:12px;text-align:center;width:auto;
            margin-top:-20px
            }	
            .Comments_Pagging td {direction:rtl;border:1px solid #dddddd;background:#f7f7f5;
            padding:2px 7px 2px 7px;margin:4px;}
            .Pagging_Active {border:1px solid #ffffff !important;background:#ffffff !important;font-weight:bold;
            color:#000000 !important;
            }
            .Pagging_Active a {text-decoration:none;color:#000000 !important;font-weight:bold;}
            .Comments_Pagging a	{text-decoration:none;color:#555553!important;}
            .Pagging_dash {
            border:1px solid #ffffff !important;
            background:#ffffff !important;
            background:url(/Story/Images/Story_Seporator.jpg) repeat-x bottom !important;
            margin-right:0px !important;
            margin-left:0px !important;
            height:16px;
            }
         </style>
         <table cellspacing=6  cellpadding=6 align=center class='Comments_Pagging'>
            <tr>
               <td><a  href='/Ext/Comp/NominationList/CdaNominationList_Iframe/1,15014,L-0-2,00.html?parms=5543' >הבא&gt;</a></td>
               <td><a  href='/Ext/Comp/NominationList/CdaNominationList_Iframe/1,15014,L-0-299,00.html?parms=5543'>299</a></td>
               <td class=Pagging_dash></td>
               <td><a  href='/Ext/Comp/NominationList/CdaNominationList_Iframe/1,15014,L-0-3,00.html?parms=5543'>3</a></td>
               <td><a  href='/Ext/Comp/NominationList/CdaNominationList_Iframe/1,15014,L-0-2,00.html?parms=5543'>2</a></td>
               <td  class=Pagging_Active>1</td>
               &nbsp;
            </tr>
         </table>
         <div dir=rtl align=center class=minu>(2990 מינויים)</div>
      </div>
   </div>
   <SCRIPT>
      function goSetHeight() {if (parent == window) return;else parent.setIframeHeight('CdaNominationList');
      
      }
      
   </SCRIPT>
</BODY> 
    """


datapackage['resources'].append({
            'path': 'data/nominations-list.csv',
            'name': 'nominations-list',
            PROP_STREAMING: True,
            'schema': {
                'fields': [
                    {'name': 'date', 'type': 'string'},
                    {'name': 'full_name', 'type': 'string'},
                    {'name': 'company', 'type': 'string'},
                    {'name': 'description', 'type': 'string'},
                    {'name': 'proof_url', 'type': 'string'},
                    {'name': 'source', 'type': 'string'}
                ]
            }
})

logging.info('datapackage: %s' % datapackage)

# datapackage['resources'].append({
#     'name': 'government-companies',
#     'path': 'data/government-companies.csv',
#     'schema': {
#         'fields': [
#             {'name': h, 'type': 'string'} for h in headers
#         ]
#     }
# })

# datapackage = {
#     'resources': [
#         {
#             'name': 'nominations-list',
#             'path': 'nominations-list.csv',
#             'schema': {
#                 'fields': [
#                     {'name': 'date', 'type': 'string'},
#                     {'name': 'title', 'type': 'string'},
#                     {'name': 'company', 'type': 'string'},
#                     {'name': 'description', 'type': 'string'},
#                     {'name': 'proof_url', 'type': 'string'}
#                 ]
#             }
#         }
#     ]
# }


spew(datapackage, [scrape()])


