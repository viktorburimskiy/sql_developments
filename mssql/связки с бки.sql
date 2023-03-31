/****** Сбор информации по закрытым при рефинансировании кредитам (RISKREQ-313)
1. Выгрузка для КК (кредитных контроллёров):
Набор полей:
	Номер заявки,
	дата заведения,
	номер контракта,
	дата выдачи +
	по данным из БКИ
	лимит
	дата выдачи,
	валюта,
	остаток основного долга
Информация в выгрузку добавляется только в том случае, если по заявке на рефин есть незакрытый внешний контракт и от даты контракта прошло больше 60 дней ******/

DECLARE @StartDate as date = '20180501'
DECLARE @EndDate as date = EOMONTH(@StartDate)

SELECT 
	[Заявка]
	,[Дата заявки]
	,[Договор]
	,[Сумма выданного рефина]
	,[Дата выдачи рефина]
	,[Кр.Лимит по заявке]
	,[Дата выдачи по заявке]
	,[Тип отношения к договору]
	,[Тип Кредита по заявке]
	,[Валюта]
	,[Лимит по БКИ]
	,[Дата выдачи по БКИ]
	,[Остаток основного долга по БКИ]
	,[Дата актуальности данных БКИ]
	,[Статус по БКИ]
	,[Итоговый статус]
	--,[Прошло дней с даты выдачи]

FROM
	(SELECT
		pd.x10_application_id as [Заявка]
		,pd.x10_request_date as [Дата заявки]
		,pd.DOG_Name as [Договор]
		,pd.DOG_Volume_RUB as [Сумма выданного рефина]
		,pd.DOG_Date_Begin as [Дата выдачи рефина]
	--,pd.CL_Name
	--,pd.CL_DateBorn
	--,pd.BKI_APP_ID
		,pd.CREDIT_LIMIT as [Кр.Лимит по заявке]
		,pd.OPEN_DATE as [Дата выдачи по заявке]
		,CASE 
			WHEN pd.RELATIONSHIP = 1 THEN 'Основной заемщик'
			WHEN pd.RELATIONSHIP = 2 THEN 'Дополнительная карта'
			WHEN pd.RELATIONSHIP = 3 THEN 'Авторизованный пользователь'
			WHEN pd.RELATIONSHIP = 4 THEN 'Совместный'
			WHEN pd.RELATIONSHIP = 5 THEN 'Поручитель'
			WHEN pd.RELATIONSHIP = 6 THEN 'Залогодатель'
			WHEN pd.RELATIONSHIP = 7 THEN 'Конкурсный управляющий'
			WHEN pd.RELATIONSHIP = 8 THEN 'Принципал'
			WHEN pd.RELATIONSHIP = 9 THEN 'Юр.лицо'
		END as [Тип отношения к договору]
		,CASE 
			WHEN pd.[TYPE] = 1 THEN 'Автокредит'
			WHEN pd.[TYPE] = 4 THEN 'Лизинг'
			WHEN pd.[TYPE] = 6 THEN 'Ипотека'
			WHEN pd.[TYPE] = 7 THEN 'Кредитная карта'
			WHEN pd.[TYPE] = 8 THEN 'Дебетовая карта с овердрафтом'
			WHEN pd.[TYPE] = 9 THEN 'Потребительский кредит'
			WHEN pd.[TYPE] = 10 THEN 'На развитие бизнеса'
			WHEN pd.[TYPE] = 11 THEN 'Пополнение оборотных средств'
			WHEN pd.[TYPE]= 12 THEN 'Покупка оборудования'
			WHEN pd.[TYPE] = 13 THEN 'Строительство недвижимости'
			WHEN pd.[TYPE] = 14 THEN 'Покупка ценных бумаг'
			WHEN pd.[TYPE] = 15 THEN 'Межбанковский кредит'
			WHEN pd.[TYPE] = 16 THEN 'Кредит на обучение'
			WHEN pd.[TYPE] = 17 THEN 'Счет телекоммуникационных услуг'
			WHEN pd.[TYPE] = 18 THEN 'Кредит мобильного оператора'
			WHEN pd.[TYPE] = 19 THEN 'Коммунальный счет'
			WHEN pd.[TYPE] = 20 THEN 'Факторинг'
			WHEN pd.[TYPE] = 21 THEN 'Микрозаем'
			WHEN pd.[TYPE] = 22 THEN 'Нецелевой кредит наличными'
			WHEN pd.[TYPE] = 50 THEN 'Просмотр счета'
			WHEN pd.[TYPE] = 99 THEN 'Неизвестно'
		END as [Тип Кредита по заявке]
		,pd.CURRENCY as [Валюта]
		,pd.RESULT_DATE as [Дата вставки]
		,CASE WHEN DATEDIFF(day, pd.DOG_Date_Begin, IsNull(cr1.CREATEDATETIME, cr2.CREATEDATETIME)) < 0
			THEN cr1.CREDITLIMIT
			ELSE IsNull(cr1.CREDITLIMIT, cr2.CREDITLIMIT)
		END as [Лимит по БКИ]
		,CASE WHEN DATEDIFF(day, pd.DOG_Date_Begin, IsNull(cr1.CREATEDATETIME, cr2.CREATEDATETIME)) < 0
			THEN cr1.OPENDATE
			ELSE IsNull(cr1.OPENDATE, cr2.OPENDATE)
		END as [Дата выдачи по БКИ]
		,CASE WHEN DATEDIFF(day, pd.DOG_Date_Begin, IsNull(cr1.CREATEDATETIME, cr2.CREATEDATETIME)) < 0
			THEN cr1.OUTSTANDING
			ELSE IsNull(cr1.OUTSTANDING, cr2.OUTSTANDING)
		END as [Остаток основного долга по БКИ]
		,CASE WHEN DATEDIFF(day, pd.DOG_Date_Begin, IsNull(cr1.CREATEDATETIME, cr2.CREATEDATETIME)) < 0
			THEN CONVERT(date, cr1.CREATEDATETIME)
			ELSE CONVERT(date, IsNull(cr1.CREATEDATETIME, cr2.CREATEDATETIME))
		END as [Дата актуальности данных БКИ]
		,CASE WHEN DATEDIFF(day, pd.DOG_Date_Begin, IsNull(cr1.CREATEDATETIME, cr2.CREATEDATETIME)) < 0
			THEN cr1.STATUS_T
			ELSE IsNull(cr1.STATUS_T, cr2.STATUS_T)
		END as [Статус по БКИ]
		,CASE 
			WHEN IsNull(cr1.TYPE_, cr2.TYPE_) = 7 and IsNull(cr1.OUTSTANDING, cr2.OUTSTANDING) = 0 THEN 'Кредит закрыт'
			WHEN (IsNull(cr1.[STATUS], cr2.[STATUS]) = 13) 
				or (IsNull(cr1.[STATUS], cr2.[STATUS]) = 12 and IsNull(cr1.OUTSTANDING, cr2.OUTSTANDING) = 0) 
				or (IsNull(cr1.[STATUS], cr2.[STATUS]) = 12 and IsNull(cr1.OUTSTANDING, cr2.OUTSTANDING) IS NULL) 
				or (IsNull(cr1.[STATUS], cr2.[STATUS]) = 14) 
				or (IsNull(cr1.[STATUS], cr2.[STATUS]) = 0 and IsNull(cr1.OUTSTANDING, cr2.OUTSTANDING) = 0) THEN 'Кредит закрыт'
			ELSE COALESCE(cr1.[STATUS_T], cr2.[STATUS_T], '')
		END [Итоговый статус]
		,DATEDIFF(day, pd.DOG_Date_Begin, IsNull(cr1.CREATEDATETIME, cr2.CREATEDATETIME)) as [Прошло дней с даты выдачи]
		-- есть дубликаты . Коммент РУстема: Когда попытались подтянуть клиента к заявке, подтянулось 2 записи.
		,ROW_NUMBER() OVER (PARTITION BY pd.x10_application_id, pd.CREDIT_LIMIT, pd.OPEN_DATE, pd.RELATIONSHIP 
								ORDER BY CASE 
											WHEN IsNull(cr1.TYPE_, cr2.TYPE_) = 7 and IsNull(cr1.OUTSTANDING, cr2.OUTSTANDING) = 0 THEN 0 --'Кредит закрыт'
											WHEN (IsNull(cr1.[STATUS], cr2.[STATUS]) = 13) 
												or (IsNull(cr1.[STATUS], cr2.[STATUS]) = 12 and IsNull(cr1.OUTSTANDING, cr2.OUTSTANDING) = 0) 
												or (IsNull(cr1.[STATUS], cr2.[STATUS]) = 12 and IsNull(cr1.OUTSTANDING, cr2.OUTSTANDING) IS NULL) 
												or (IsNull(cr1.[STATUS], cr2.[STATUS]) = 14) 
												or (IsNull(cr1.[STATUS], cr2.[STATUS]) = 0 and IsNull(cr1.OUTSTANDING, cr2.OUTSTANDING) = 0) THEN 0 --'Кредит закрыт'
											ELSE 1
										END, IsNull(cr1.CREATEDATETIME, cr2.CREATEDATETIME) DESC) as rn
	--, cr2.FIO
	--, cr1.APPLICATIONUID
	FROM
		(	SELECT let.APPLICATIONID as x10_application_id
				,pd.APPLICATIONDATE as x10_request_date
				,CONVERT(money, let.CREDIT_LIMIT) as CREDIT_LIMIT
				,let.CURRENCY
				,let.OPEN_DATE	
				,let.OUTSTANDING
				,let.RELATIONSHIP
				--,CASE WHEN let.[TYPE] IN (10, 16, 21, 22) THEN 9 ELSE let.[TYPE] END as [TYPE]
				,let.[TYPE]
				,let.IMPACT_ON_DEBT
				,REPLACE(REPLACE(let.COMMENT, CHAR(13), ''), CHAR(10), '') as COMMENT
				,c.DOG_Name
				,c.DOG_Volume_RUB
				,c.DOG_Date_Begin
				,cl.CL_Name
				,cl.CL_DateBorn
				,ref.BKI_APP_ID
				,ref.BKI_GROUP_ID
				,ref.DEAL_NUM
				,ref.INT_STATUS
				,ref.RESULT_DATE --Дата вставки
			FROM  AutoClassic.dbo. ACLS_ALET as let
			LEFT JOIN AutoClassic.dbo.ACLS_AAPD as pd on pd.APPLICATIONID = let.APPLICATIONID
			LEFT JOIN FirebirdCopy.dbo.contract_request_cb as cb on let.APPLICATIONID = cb.x10_application_id
			LEFT JOIN FirebirdCopy.dbo.Contract as c on c.ID = cb.ID
			LEFT JOIN FirebirdCopy.dbo.Client_info as cl on cl.ID = c.Client_ID
			LEFT JOIN 
				(	SELECT *
						-- 
						, CASE WHEN BKI_APP_ID IS NULL 
							THEN ROW_NUMBER() OVER (PARTITION BY APPLICATION_ID ORDER BY CASE WHEN BKI_APP_ID IS NULL THEN 0 ELSE 1 END DESC)
							ELSE 1 
						END as rn
					FROM  Temp_data.BurimskyVA.BKI_REFIN_FOR_SRM 
				) as ref on ref.APPLICATION_ID = let.APPLICATIONID and ref.rn = 1
			WHERE c.DOG_Date_Begin BETWEEN @StartDate and @EndDate
				and ((IMPACT_ON_DEBT = 3) or (IMPACT_ON_DEBT IS NULL and COMMENT LIKE '%РЕФ%' and COMMENT NOT LIKE '%не будет%'))

		) as pd
	-- для join'а по APPLICATIONUID
	LEFT JOIN
		(	SELECT *
			FROM
				(	SELECT 
						m.APPLICATIONUID
						,m.GROUPID
						,m.CREATEDATETIME -- [Дата составления отчета]
						,l.OPENDATE -- [Дата открытия договора]
						,l.CREDITLIMIT--  [Кредитный лимит]
						,l.RELATIONSHIP
						,CASE WHEN l.TYPE_ IN (10, 16, 21, 22) THEN 9 ELSE l.TYPE_ END as TYPE_
						,l.CURRENCY
						,l.STATUS
						,CASE
							WHEN l.STATUS =0  THEN 'Активный'
							WHEN l.STATUS =12 THEN 'Оплачен за счет обеспечения'
							WHEN l.STATUS =13 THEN 'Счет закрыт'
							WHEN l.STATUS =14 THEN 'Передан на обслуживание в другой банк'
							WHEN l.STATUS =21 THEN 'Спор'
							WHEN l.STATUS =52 THEN 'Просрочен'
							WHEN l.STATUS =61 THEN 'Проблемы с возвратом'
						END as STATUS_T
						,l.OUTSTANDING -- [Оставшаяся непогашенная задолженность]
						,ROW_NUMBER() OVER (PARTITION BY m.APPLICATIONUID, l.OPENDATE, l.CREDITLIMIT, l.CURRENCY, l.RELATIONSHIP 
												ORDER BY CASE WHEN m.GROUPID IS NOT NULL THEN 0 ELSE 1 END, m.CREATEDATETIME DESC, l.OUTSTANDING) as rn1
					FROM  CreditRegistry.dbo.SF_MainType m
					LEFT JOIN CreditRegistry.dbo.SF_SINGLEFORMATType s on s.MAIN=m.Hjid 
					LEFT JOIN CreditRegistry.dbo.SF_LoansType l on s.LOANS=l.LoansTypes_LOAN_Hjid
					--так будет работать быстрее
					WHERE m.APPLICATIONUID IN (SELECT DISTINCT BKI_APP_ID FROM Temp_data.BurimskyVA.BKI_REFIN_FOR_SRM)			
				) as t
			WHERE t.rn1 = 1
		) as cr1 on cr1.APPLICATIONUID = pd.BKI_APP_ID
			and cr1.CREDITLIMIT = pd.CREDIT_LIMIT
			and cr1.OPENDATE = pd.OPEN_DATE
			and cr1.RELATIONSHIP = pd.RELATIONSHIP
	-- для join'а по ФИО и Дате рождения (если нет совпадений по APPLICATIONUID)
	LEFT JOIN
		(
			SELECT *
			FROM
				(	SELECT 
						nt.FIO
						,nt.BIRTHDATE
						,m.APPLICATIONUID
						,m.GROUPID
						,m.CREATEDATETIME -- [Дата составления отчета]
						,l.OPENDATE -- [Дата открытия договора]
						,CONVERT(money, l.CREDITLIMIT) as CREDITLIMIT
						,l.RELATIONSHIP
						,CASE WHEN l.TYPE_ IN (10, 16, 21, 22) THEN 9 ELSE l.TYPE_ END as TYPE_
						,l.CURRENCY
						,l.STATUS
						,CASE
							WHEN l.STATUS =0  THEN 'Активный'
							WHEN l.STATUS =12 THEN 'Оплачен за счет обеспечения'
							WHEN l.STATUS =13 THEN 'Счет закрыт'
							WHEN l.STATUS =14 THEN 'Передан на обслуживание в другой банк'
							WHEN l.STATUS =21 THEN 'Спор'
							WHEN l.STATUS =52 THEN 'Просрочен'
							WHEN l.STATUS =61 THEN 'Проблемы с возвратом'
						END as STATUS_T
						,l.OUTSTANDING -- [Оставшаяся непогашенная задолженность]
						,ROW_NUMBER() OVER (PARTITION BY nt.FIO, nt.BIRTHDATE,  l.OPENDATE, l.CREDITLIMIT, l.CURRENCY, l.RELATIONSHIP 
												ORDER BY  CASE WHEN m.GROUPID IS NOT NULL THEN 0 ELSE 1 END, m.CREATEDATETIME DESC, l.OUTSTANDING) as rn2
					FROM  CreditRegistry.dbo.SF_MainType m
					LEFT JOIN CreditRegistry.dbo.SF_SINGLEFORMATType s on s.MAIN=m.Hjid 
					LEFT JOIN CreditRegistry.dbo.SF_LoansType l on s.LOANS=l.LoansTypes_LOAN_Hjid
					LEFT JOIN 
						(	SELECT LASTNAME + ' ' + FIRSTNAME + ' ' + SECONDNAME  as FIO
								, BIRTHDATE
								, NameTypes_NAME__Hjid
								, ROW_NUMBER() OVER (PARTITION BY NameTypes_NAME__Hjid ORDER BY NameTypes_NAME__Hjid) as rn
							FROM CreditRegistry.dbo.SF_NameType 
						)nt on s.NAMES_ = nt.NameTypes_NAME__Hjid and nt.rn = 1
					WHERE nt.FIO IN (SELECT DISTINCT cl.CL_Name
									FROM  AutoClassic.dbo. ACLS_ALET as let
									LEFT JOIN FirebirdCopy.dbo.contract_request_cb as cb on let.APPLICATIONID = cb.x10_application_id
									LEFT JOIN FirebirdCopy.dbo.Contract as c on c.ID = cb.ID
									LEFT JOIN FirebirdCopy.dbo.Client_info as cl on cl.ID = c.Client_ID
									WHERE c.DOG_Date_Begin BETWEEN @StartDate and @EndDate
											and ((IMPACT_ON_DEBT = 3) or 
											(IMPACT_ON_DEBT IS NULL and COMMENT LIKE '%РЕФ%' and COMMENT NOT LIKE '%не будет%')))
				) as t
			WHERE t.rn2 = 1
		) as cr2 on REPLACE(REPLACE(cr2.FIO, ' ', ''),'-', '') = REPLACE(REPLACE(pd.CL_Name, ' ', ''),'-', '')
			and cr2.BIRTHDATE = pd.CL_DateBorn
			and cr2.CREDITLIMIT = pd.CREDIT_LIMIT
			and cr2.OPENDATE = pd.OPEN_DATE
			and cr2.RELATIONSHIP = pd.RELATIONSHIP
	) t
WHERE t.rn = 1
	--and ([Прошло дней с даты выдачи] > 60 or [Прошло дней с даты выдачи] < 0)
	--and ([Дата заявки] <= [Дата актуальности данных БКИ])-- or [Дата актуальности данных БКИ] IS NULL)
	and [Итоговый статус] <> 'Кредит закрыт'