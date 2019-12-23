#Description of Maya Files and Fields

## 1. maya_company_officer_list

* Maya Form Number: ת097
* Title: "דוח מיידי על מצבת נושאי משרה בכירה"

The scraper parses the table of company officers in each form and generates a line per person in the original form. 
When people have multiple jobs in the company they will appear multiple times.
* Fields:

|  Title | Meaning  |  
|---|---|
| date  | date notification was posted to maya  |
| url   | original url data was scraped from |
 | id | אסמכתא של הטופס |
 | type | סוג הטופס | 
 | fix_for | אם הטופס מתקן טופס קודם אז זו האסמכתא של הטופס אותו מתקנים | 
 | perv_doc| המזהה של הטופס הקודם לחברה זו מהסוג הזה כשזה ריק זה הטופס הראשון שנסרק |
 | next_doc| המזהה של הטופס הבא לחברה זו מהסוג הזה כשזה ריק זה הטופס האחרון והעדכני ביותר שנסרק לחברה |
 |CompanyName| שם החברה בעברית | 
 |CompanyUrl | עמוד הבית של החברה אם ניתן | 
 |HeaderMisparBaRasham | מספר החברה ברשם | 
 |HeaderSemelBursa | סמל החברה בבורסא | 
 |KodSugYeshut | לא יודע | 
 |MezahehHotem |  לא בטוח.. נראה כמזהה של מי שמילא את הטופס במערכת | 
 |MezahehTofes | סוג הטופס| 
 |NeyarotErechReshumim | האם לחברה יש נירות ערך בבורסת תל אביב  | 
 |PumbiLoPumbi |  האם הטופס פומבי או לא| 
 |PreviousCompanyNames | שמות עבר של החברה | 
 |TaarichIdkunMivne | התאריך בו עודכן מבנה הטופס | 
 | MezahehYeshut| מזהה של החברה |
 | FullName | שם  נושא  המשרה בעברית |
 | FullNameEn  שם נושא  המשרה באנגלית| |									
 | AppointmentDate | מועד המינוי (לא ממולא לרוב) |
 | SugMisparZihui |, אחד מ `מספר זיהוי לא ידוע`,`מספר תעודת זהות`,`מספר מזהה אחר`, `מספר דרכון` ,`מספר ברשם החברות בישראל`,`מספר רשם בארץ ההתאגדות בחו"ל`, `מספר ברשם השותפויות בישראל`|
 | MisparZihui| מספר הזהות / מספר דרכון של נושא המשרה (בעתי הקרוב נתחיל להסתיר את המספרים האמיתיים) |									
 | Position| התפקיד של נושא המשרה  |
 | PositionDetails | פרטים על התפקיד ביחוד אם בחרו אחר לתפקיד | 
 | CompensationCommittee | ועדת תגמול |									
 | IsFinancialExpert | בעל מומחיות חשבונאית פיננסית |
 | IsInspectionComitee| ועדת ביקורת | 
 | IsOftheAuditCommittee| ועדת מאזן | 
 | OtherCommittees | ועדות נוספות|									
 | IndependentDirectorStartDate|  מועד סיווגו כדב"ת (דירקטור בלתי תלוי)|
 
## 2. maya_stakeholder_list

* Maya Form Number: ת077
* Title: "דוח מיידי על מצבת החזקות בעלי עניין ונושאי משרה בכירה"

stakeholders can hold different types of stocks in different quantities. In this file they will appear multiple times 
with details about the stock they are holding.
 
* Fields:
    
    |  Title | Meaning  |  
    |---|---|
    | date  | date notification was posted to maya  |
    | url   | original url data was scraped from |
     | id | אסמכתא של הטופס |
     | type | סוג הטופס | 
     | fix_for | אם הטופס מתקן טופס קודם אז זו האסמכתא של הטופס אותו מתקנים | 
     | perv_doc| המזהה של הטופס הקודם לחברה זו מהסוג הזה כשזה ריק זה הטופס הראשון שנסרק |
     | next_doc| המזהה של הטופס הבא לחברה זו מהסוג הזה כשזה ריק זה הטופס האחרון והעדכני ביותר שנסרק לחברה |
     |CompanyName| שם החברה בעברית | 
     |CompanyUrl | עמוד הבית של החברה אם כתבו | 
     |HeaderMisparBaRasham | מספר החברה ברשם | 
     |HeaderSemelBursa | סמל החברה בבורסא | 
     |KodSugYeshut | לא יודע | 
     |MezahehHotem |  לא בטוח.. נראה כמזהה של מי שמילא את הטופס במערכת | 
     |MezahehTofes | סוג הטופס| 
     |NeyarotErechReshumim | האם לחברה יש נירות ערך בבורסת תל אביב  | 
     |PumbiLoPumbi |  האם הטופס פומבי או לא| 
     |PreviousCompanyNames | שמות עבר של החברה | 
     |TaarichIdkunMivne | התאריך בו עודכן מבנה הטופס | 
     | MezahehYeshut| מזהה של החברה |
     | stakeholder_type |    האם מדובר בבעל עניין שיש לו לפחות חמש אחוז בחברה או נושא משרה שיש לו פחות מחמש אחוז  |  
     | FullName | שם בעברית |
     | FullNameEn  שם באנגלית| |									
     | SugMisparZihui |, אחד מ `מספר זיהוי לא ידוע`,`מספר תעודת זהות`,`מספר מזהה אחר`, `מספר דרכון` ,`מספר ברשם החברות בישראל`,`מספר רשם בארץ ההתאגדות בחו"ל`, `מספר ברשם השותפויות בישראל`|
     | MisparZihui| מספר הזהות / מספר דרכון של נושא המשרה (בעתי הקרוב נתחיל להסתיר את המספרים האמיתיים) |									
     | Nationality | ארץ אזרחות / התאגדות או רישום|
     | IsFrontForOthers | האם המחזיק משמש נציג לצורך הדיווח של מספר בעלי מניות המחזיקים ביחד עמו בניירות ערך של תאגיד|   
     | TreasuryShares | האם המניות המוחזקות הינן מניות רדומות |
     | PreviousAmount | יתרה בדיווח מרכז קודם (כמות ניירות ערך)|    
     | ChangeSincePrevious | שינוי בכמות ניירות הערך|   
     | MisparNiarErech | מספר נייר ערך בבורסה|
     | MaximumRetentionRate | שיעור החזקה מרבי של המחזיק בנייר הערך בתקופת הדוח|   
     | MinimumRetentionRate | שיעור החזקה מזערי של המחזיק בנייר הערך בתקופת הדוח|   
     | Notes | הערות בטקסט חופשי|
     | HolderOwner| מי שולט בבעל העניין|
     |AccumulateHoldings |האם המחזיק רשאי לדווח על השינוי בהחזקה באופן מצטבר |
     | IsRequiredToReportChange| האם המחזיק חייב על פי דין לדווח על כל שינוי בהחזקה|   
     | StockName| שם, סוג וסדרה של נייר ערך	|
     | CurrentAmount| כמות ניירות ערך מעודכנת	|
     | CapitalPct| שיעור החזקה  הון %|
     | VotePower | שיעור החזקה הצבעה %|
     | CapitalPct_Dilul| שיעור החזקה  הון % בדילול מלא|
     |VotePower_Dilul | שיעור החזקה הצבעה % בדילול מלא|
                        
## 3. reported_acedemic_degrees

* Maya Form Number: ת91/93
* Title: "דוח מיידי על מינוי נושא משרה בכירה (למעט מינוי דירקטור/ יו"ר דירקטוריון ולמעט יחיד שמונה מטעם תאגיד שהוא דירקטור)"

when appointing a new officer a company will list the academic degrees the person has. This is this list
 
* Fields:
    
    |  Title | Meaning  |  
    |---|---|
    | date  | date notification was posted to maya  |    																
    | url   | original url data was scraped from |
    | FullName | שם בעברית |
    | FullNameEn |  שם באנגלית| 
    | SugMisparZihui |, אחד מ `מספר זיהוי לא ידוע`,`מספר תעודת זהות`,`מספר מזהה אחר`, `מספר דרכון` ,`מספר ברשם החברות בישראל`,`מספר רשם בארץ ההתאגדות בחו"ל`, `מספר ברשם השותפויות בישראל`|
    | MisparZihui| מספר הזהות / מספר דרכון של נושא המשרה (בעתי הקרוב נתחיל להסתיר את המספרים האמיתיים) |	
| Degree| סוג התואר|
| Field| תחום|
| Institution| שם המוסד |
              
## 4. reported_work_record

* Maya Form Number: ת91/93
* Title: "דוח מיידי על מינוי נושא משרה בכירה (למעט מינוי דירקטור/ יו"ר דירקטוריון ולמעט יחיד שמונה מטעם תאגיד שהוא דירקטור)"

when appointing a new officer a company will list the previous work expirence the person has. This is this list

 
* Fields:
    
    |  Title | Meaning  |  
    |---|---|
    | date  | date notification was posted to maya  |    																
    | url   | original url data was scraped from |
     | FullName | שם בעברית |
     | FullNameEn | שם באנגלית| 
     | SugMisparZihui |, אחד מ `מספר זיהוי לא ידוע`,`מספר תעודת זהות`,`מספר מזהה אחר`, `מספר דרכון` ,`מספר ברשם החברות בישראל`,`מספר רשם בארץ ההתאגדות בחו"ל`, `מספר ברשם השותפויות בישראל`|
     | MisparZihui| מספר הזהות / מספר דרכון של נושא המשרה (בעתי הקרוב נתחיל להסתיר את המספרים האמיתיים) |	
    | Position|  התפקיד כפי שנכתב בטופס|
    | CompanyName| שם החברה כפי שנכתב בטופס|
    | DurationInPosition/When| משך הזמן שמילא בתפקיד זה שדה חופשי שמכיל לעיתים טווח שנים, לפעמים מספר וכו | 
         
## 5. maya_holdings_change

* Maya Form Number: ת086
* Title: "דוח מיידי על שינוי בהחזקה עצמית של תעודות התחייבות וכתבי אופציה של התאגיד"
    
    |  Title | Meaning  |  
    |---|---|
    | date  | date notification was posted to maya  |    																
    | url   | original url data was scraped from |    
     | id | אסמכתא של הטופס |
     | type | סוג הטופס | 
     | fix_for | אם הטופס מתקן טופס קודם אז זו האסמכתא של הטופס אותו מתקנים | 
     | perv_doc| המזהה של הטופס הקודם לחברה זו מהסוג הזה כשזה ריק זה הטופס הראשון שנסרק |
     | next_doc| המזהה של הטופס הבא לחברה זו מהסוג הזה כשזה ריק זה הטופס האחרון והעדכני ביותר שנסרק לחברה |
     | company| שם החברה בעברית |
     |CompanyUrl | עמוד הבית של החברה אם כתבו | 
     |HeaderMisparBaRasham | מספר החברה ברשם | 
     |HeaderSemelBursa | סמל החברה בבורסא | 
     |MezahehHotem |  לא בטוח.. נראה כמזהה של מי שמילא את הטופס במערכת | 
     |MezahehTofes | סוג הטופס| 
     |NeyarotErechReshumim | האם לחברה יש נירות ערך בבורסת תל אביב  | 
     |PumbiLoPumbi |  האם הטופס פומבי או לא| 
     |PreviousCompanyNames | שמות עבר של החברה | 
     |IsHeldBySubsidiary| האם המחזיק הוא החברה עצמה או חברת בת |
     |SubsidiaryName | שם חברת הבת|
     |SubsidiaryNameEn | שם חברת הבת באנגלית |
     |PercentageHeld | כמות האחזקה באחוזים|
    | StockNumber| מס' ניר הערך|
    | ChangedStockName| שם ניר הערך|
    | ChangeDate | תאריך השינוי|
    | StockAmountChange| כמות השינוי במספר מניות|
    | NumberOfStocksAfterChange| מספר המניות שנותרו לאחר השינוי|	
    | CitizenshipOfRegistering|  אזרחות של רישום החברה|
    | countryOfRegistering| ארץ רישום החברה|
    | Proceeds| הסכום שהתקבל (לא קיים תמיד)|
    | ChangeExplanation| הסבר לשינוי|   
    | Subsidiary_IDType| סוג מספר הזיהוי של חברה הבת|
    | Subsidiary_IDNumber| מספר הזהוי של חברת הבת|           	
   
       		
## טפסים בעבודה 
דוח מיידי על מינוי נושא משרה בכירה   
שינוי החזקות ב"ע/נ.משרה.     
 רכישה/מכירת אג"ח ב"ע/נ.משרה
  
 
 