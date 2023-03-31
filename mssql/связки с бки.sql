/****** ���� ���������� �� �������� ��� ���������������� �������� (RISKREQ-313)
1. �������� ��� �� (��������� �����������):
����� �����:
	����� ������,
	���� ���������,
	����� ���������,
	���� ������ +
	�� ������ �� ���
	�����
	���� ������,
	������,
	������� ��������� �����
���������� � �������� ����������� ������ � ��� ������, ���� �� ������ �� ����� ���� ���������� ������� �������� � �� ���� ��������� ������ ������ 60 ���� ******/

DECLARE @StartDate as date = '20180501'
DECLARE @EndDate as date = EOMONTH(@StartDate)

SELECT 
	[������]
	,[���� ������]
	,[�������]
	,[����� ��������� ������]
	,[���� ������ ������]
	,[��.����� �� ������]
	,[���� ������ �� ������]
	,[��� ��������� � ��������]
	,[��� ������� �� ������]
	,[������]
	,[����� �� ���]
	,[���� ������ �� ���]
	,[������� ��������� ����� �� ���]
	,[���� ������������ ������ ���]
	,[������ �� ���]
	,[�������� ������]
	--,[������ ���� � ���� ������]

FROM
	(SELECT
		pd.x10_application_id as [������]
		,pd.x10_request_date as [���� ������]
		,pd.DOG_Name as [�������]
		,pd.DOG_Volume_RUB as [����� ��������� ������]
		,pd.DOG_Date_Begin as [���� ������ ������]
	--,pd.CL_Name
	--,pd.CL_DateBorn
	--,pd.BKI_APP_ID
		,pd.CREDIT_LIMIT as [��.����� �� ������]
		,pd.OPEN_DATE as [���� ������ �� ������]
		,CASE 
			WHEN pd.RELATIONSHIP = 1 THEN '�������� �������'
			WHEN pd.RELATIONSHIP = 2 THEN '�������������� �����'
			WHEN pd.RELATIONSHIP = 3 THEN '�������������� ������������'
			WHEN pd.RELATIONSHIP = 4 THEN '����������'
			WHEN pd.RELATIONSHIP = 5 THEN '����������'
			WHEN pd.RELATIONSHIP = 6 THEN '������������'
			WHEN pd.RELATIONSHIP = 7 THEN '���������� �����������'
			WHEN pd.RELATIONSHIP = 8 THEN '���������'
			WHEN pd.RELATIONSHIP = 9 THEN '��.����'
		END as [��� ��������� � ��������]
		,CASE 
			WHEN pd.[TYPE] = 1 THEN '����������'
			WHEN pd.[TYPE] = 4 THEN '������'
			WHEN pd.[TYPE] = 6 THEN '�������'
			WHEN pd.[TYPE] = 7 THEN '��������� �����'
			WHEN pd.[TYPE] = 8 THEN '��������� ����� � �����������'
			WHEN pd.[TYPE] = 9 THEN '��������������� ������'
			WHEN pd.[TYPE] = 10 THEN '�� �������� �������'
			WHEN pd.[TYPE] = 11 THEN '���������� ��������� �������'
			WHEN pd.[TYPE]= 12 THEN '������� ������������'
			WHEN pd.[TYPE] = 13 THEN '������������� ������������'
			WHEN pd.[TYPE] = 14 THEN '������� ������ �����'
			WHEN pd.[TYPE] = 15 THEN '������������� ������'
			WHEN pd.[TYPE] = 16 THEN '������ �� ��������'
			WHEN pd.[TYPE] = 17 THEN '���� �������������������� �����'
			WHEN pd.[TYPE] = 18 THEN '������ ���������� ���������'
			WHEN pd.[TYPE] = 19 THEN '������������ ����'
			WHEN pd.[TYPE] = 20 THEN '���������'
			WHEN pd.[TYPE] = 21 THEN '���������'
			WHEN pd.[TYPE] = 22 THEN '��������� ������ ���������'
			WHEN pd.[TYPE] = 50 THEN '�������� �����'
			WHEN pd.[TYPE] = 99 THEN '����������'
		END as [��� ������� �� ������]
		,pd.CURRENCY as [������]
		,pd.RESULT_DATE as [���� �������]
		,CASE WHEN DATEDIFF(day, pd.DOG_Date_Begin, IsNull(cr1.CREATEDATETIME, cr2.CREATEDATETIME)) < 0
			THEN cr1.CREDITLIMIT
			ELSE IsNull(cr1.CREDITLIMIT, cr2.CREDITLIMIT)
		END as [����� �� ���]
		,CASE WHEN DATEDIFF(day, pd.DOG_Date_Begin, IsNull(cr1.CREATEDATETIME, cr2.CREATEDATETIME)) < 0
			THEN cr1.OPENDATE
			ELSE IsNull(cr1.OPENDATE, cr2.OPENDATE)
		END as [���� ������ �� ���]
		,CASE WHEN DATEDIFF(day, pd.DOG_Date_Begin, IsNull(cr1.CREATEDATETIME, cr2.CREATEDATETIME)) < 0
			THEN cr1.OUTSTANDING
			ELSE IsNull(cr1.OUTSTANDING, cr2.OUTSTANDING)
		END as [������� ��������� ����� �� ���]
		,CASE WHEN DATEDIFF(day, pd.DOG_Date_Begin, IsNull(cr1.CREATEDATETIME, cr2.CREATEDATETIME)) < 0
			THEN CONVERT(date, cr1.CREATEDATETIME)
			ELSE CONVERT(date, IsNull(cr1.CREATEDATETIME, cr2.CREATEDATETIME))
		END as [���� ������������ ������ ���]
		,CASE WHEN DATEDIFF(day, pd.DOG_Date_Begin, IsNull(cr1.CREATEDATETIME, cr2.CREATEDATETIME)) < 0
			THEN cr1.STATUS_T
			ELSE IsNull(cr1.STATUS_T, cr2.STATUS_T)
		END as [������ �� ���]
		,CASE 
			WHEN IsNull(cr1.TYPE_, cr2.TYPE_) = 7 and IsNull(cr1.OUTSTANDING, cr2.OUTSTANDING) = 0 THEN '������ ������'
			WHEN (IsNull(cr1.[STATUS], cr2.[STATUS]) = 13) 
				or (IsNull(cr1.[STATUS], cr2.[STATUS]) = 12 and IsNull(cr1.OUTSTANDING, cr2.OUTSTANDING) = 0) 
				or (IsNull(cr1.[STATUS], cr2.[STATUS]) = 12 and IsNull(cr1.OUTSTANDING, cr2.OUTSTANDING) IS NULL) 
				or (IsNull(cr1.[STATUS], cr2.[STATUS]) = 14) 
				or (IsNull(cr1.[STATUS], cr2.[STATUS]) = 0 and IsNull(cr1.OUTSTANDING, cr2.OUTSTANDING) = 0) THEN '������ ������'
			ELSE COALESCE(cr1.[STATUS_T], cr2.[STATUS_T], '')
		END [�������� ������]
		,DATEDIFF(day, pd.DOG_Date_Begin, IsNull(cr1.CREATEDATETIME, cr2.CREATEDATETIME)) as [������ ���� � ���� ������]
		-- ���� ��������� . ������� �������: ����� ���������� ��������� ������� � ������, ����������� 2 ������.
		,ROW_NUMBER() OVER (PARTITION BY pd.x10_application_id, pd.CREDIT_LIMIT, pd.OPEN_DATE, pd.RELATIONSHIP 
								ORDER BY CASE 
											WHEN IsNull(cr1.TYPE_, cr2.TYPE_) = 7 and IsNull(cr1.OUTSTANDING, cr2.OUTSTANDING) = 0 THEN 0 --'������ ������'
											WHEN (IsNull(cr1.[STATUS], cr2.[STATUS]) = 13) 
												or (IsNull(cr1.[STATUS], cr2.[STATUS]) = 12 and IsNull(cr1.OUTSTANDING, cr2.OUTSTANDING) = 0) 
												or (IsNull(cr1.[STATUS], cr2.[STATUS]) = 12 and IsNull(cr1.OUTSTANDING, cr2.OUTSTANDING) IS NULL) 
												or (IsNull(cr1.[STATUS], cr2.[STATUS]) = 14) 
												or (IsNull(cr1.[STATUS], cr2.[STATUS]) = 0 and IsNull(cr1.OUTSTANDING, cr2.OUTSTANDING) = 0) THEN 0 --'������ ������'
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
				,ref.RESULT_DATE --���� �������
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
				and ((IMPACT_ON_DEBT = 3) or (IMPACT_ON_DEBT IS NULL and COMMENT LIKE '%���%' and COMMENT NOT LIKE '%�� �����%'))

		) as pd
	-- ��� join'� �� APPLICATIONUID
	LEFT JOIN
		(	SELECT *
			FROM
				(	SELECT 
						m.APPLICATIONUID
						,m.GROUPID
						,m.CREATEDATETIME -- [���� ����������� ������]
						,l.OPENDATE -- [���� �������� ��������]
						,l.CREDITLIMIT--  [��������� �����]
						,l.RELATIONSHIP
						,CASE WHEN l.TYPE_ IN (10, 16, 21, 22) THEN 9 ELSE l.TYPE_ END as TYPE_
						,l.CURRENCY
						,l.STATUS
						,CASE
							WHEN l.STATUS =0  THEN '��������'
							WHEN l.STATUS =12 THEN '������� �� ���� �����������'
							WHEN l.STATUS =13 THEN '���� ������'
							WHEN l.STATUS =14 THEN '������� �� ������������ � ������ ����'
							WHEN l.STATUS =21 THEN '����'
							WHEN l.STATUS =52 THEN '���������'
							WHEN l.STATUS =61 THEN '�������� � ���������'
						END as STATUS_T
						,l.OUTSTANDING -- [���������� ������������ �������������]
						,ROW_NUMBER() OVER (PARTITION BY m.APPLICATIONUID, l.OPENDATE, l.CREDITLIMIT, l.CURRENCY, l.RELATIONSHIP 
												ORDER BY CASE WHEN m.GROUPID IS NOT NULL THEN 0 ELSE 1 END, m.CREATEDATETIME DESC, l.OUTSTANDING) as rn1
					FROM  CreditRegistry.dbo.SF_MainType m
					LEFT JOIN CreditRegistry.dbo.SF_SINGLEFORMATType s on s.MAIN=m.Hjid 
					LEFT JOIN CreditRegistry.dbo.SF_LoansType l on s.LOANS=l.LoansTypes_LOAN_Hjid
					--��� ����� �������� �������
					WHERE m.APPLICATIONUID IN (SELECT DISTINCT BKI_APP_ID FROM Temp_data.BurimskyVA.BKI_REFIN_FOR_SRM)			
				) as t
			WHERE t.rn1 = 1
		) as cr1 on cr1.APPLICATIONUID = pd.BKI_APP_ID
			and cr1.CREDITLIMIT = pd.CREDIT_LIMIT
			and cr1.OPENDATE = pd.OPEN_DATE
			and cr1.RELATIONSHIP = pd.RELATIONSHIP
	-- ��� join'� �� ��� � ���� �������� (���� ��� ���������� �� APPLICATIONUID)
	LEFT JOIN
		(
			SELECT *
			FROM
				(	SELECT 
						nt.FIO
						,nt.BIRTHDATE
						,m.APPLICATIONUID
						,m.GROUPID
						,m.CREATEDATETIME -- [���� ����������� ������]
						,l.OPENDATE -- [���� �������� ��������]
						,CONVERT(money, l.CREDITLIMIT) as CREDITLIMIT
						,l.RELATIONSHIP
						,CASE WHEN l.TYPE_ IN (10, 16, 21, 22) THEN 9 ELSE l.TYPE_ END as TYPE_
						,l.CURRENCY
						,l.STATUS
						,CASE
							WHEN l.STATUS =0  THEN '��������'
							WHEN l.STATUS =12 THEN '������� �� ���� �����������'
							WHEN l.STATUS =13 THEN '���� ������'
							WHEN l.STATUS =14 THEN '������� �� ������������ � ������ ����'
							WHEN l.STATUS =21 THEN '����'
							WHEN l.STATUS =52 THEN '���������'
							WHEN l.STATUS =61 THEN '�������� � ���������'
						END as STATUS_T
						,l.OUTSTANDING -- [���������� ������������ �������������]
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
											(IMPACT_ON_DEBT IS NULL and COMMENT LIKE '%���%' and COMMENT NOT LIKE '%�� �����%')))
				) as t
			WHERE t.rn2 = 1
		) as cr2 on REPLACE(REPLACE(cr2.FIO, ' ', ''),'-', '') = REPLACE(REPLACE(pd.CL_Name, ' ', ''),'-', '')
			and cr2.BIRTHDATE = pd.CL_DateBorn
			and cr2.CREDITLIMIT = pd.CREDIT_LIMIT
			and cr2.OPENDATE = pd.OPEN_DATE
			and cr2.RELATIONSHIP = pd.RELATIONSHIP
	) t
WHERE t.rn = 1
	--and ([������ ���� � ���� ������] > 60 or [������ ���� � ���� ������] < 0)
	--and ([���� ������] <= [���� ������������ ������ ���])-- or [���� ������������ ������ ���] IS NULL)
	and [�������� ������] <> '������ ������'