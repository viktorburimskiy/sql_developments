SELECT *
FROM dbc.DBCInfoV
WHERE infokey = 'VERSION'


/*��������� �������*/
SELECT *
FROM dbc.tables
WHERE 1=1
	--AND tablename = 'dic_branch_urf_new2';
	AND DatabaseName in ('tst')
	--AND CreatorName = 'VABurimskiy'

/*��������� ���������*/
SELECT *
FROM sys_calendar.CALENDAR


SELECT *
FROM "001_mis_retail_channel_lab".BurimskiyV_test01


SHOW TABLE "001_mis_retail_channel_lab".BurimskiyV_test01
DROP TABLE "001_mis_retail_channel_lab".BurimskiyV_test01

--��� ����� ������� � �����������
SELECT '���������: ' || Coalesce(ClientProgramName,'') ||Chr(10) ||
						'IP: ' || Coalesce(ClientIpAddress,'')  AS cr_pk_info,
						ClientSystemUserId AS cr_pk_user			 
			  
			FROM dbc.SessionInfoV WHERE SessionNo = SESSION;
