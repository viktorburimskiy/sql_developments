declare 
@SSASDBName nvarchar(255) = 'INDICATORS'
,@XMLA nvarchar(MAX)
,@SQL nvarchar(MAX)
,@XMLAResult xml

select @XMLA = '<Batch xmlns="http://schemas.microsoft.com/analysisservices/2003/engine" Transaction="true">
<Discover xmlns="urn:schemas-microsoft-com:xml-analysis">  
   <RequestType>DISCOVER_XML_METADATA</RequestType>  
   <Restrictions>
       <RestrictionList xmlns="urn:schemas-microsoft-com:xml-analysis">
           <DatabaseID>'+@SSASDBName+'</DatabaseID>
       </RestrictionList>
   </Restrictions>
   <Properties/>
</Discover></Batch>'

if OBJECT_ID (N'tempdb..#t') is not null
	drop table #t
create table #t (
	x xml
)

select @SQL = '
insert into #t (x)
select * 
from openquery ([OLAP_INDICATORS],'''+ @XMLA + ''')'

exec (@SQL)

select @XMLAResult = x from #t

if OBJECT_ID (N'tempdb..#role_membere') is not null
	drop table #role_membere

;with XMLNAMESPACES (default 'urn:schemas-microsoft-com:xml-analysis:rowset')
select
    SSAS_DB = X_DB.x.query('./ID').value('.','nvarchar(255)'),
    RoleID = X_Roles.x.query('./ID').value('.','nvarchar(255)'),
	RoleNm = X_Roles.x.query('./Name').value('.','nvarchar(255)'),
    RoleMembe = X_Members.x.query('.').value('.','nvarchar(255)')
into #role_membere	
from @XMLAResult.nodes('//Database') X_DB(x)
		cross apply @XMLAResult.nodes('//Roles/Role') X_Roles(x)
		cross apply X_Roles.x.nodes('./Members/Member/Name') X_Members(x)

--====================================Обратно собираем XML============================

--В один XMLA
select 
	CAST(
			'<Alter ObjectExpansion="ExpandFull" xmlns="http://schemas.microsoft.com/analysisservices/2003/engine">' + 
				'<Object>
					<RoleID>' +  t2.RoleID + '</RoleID>
					<DatabaseID>' + t2.SSAS_DB + '</DatabaseID>
				</Object>'+
				'<ObjectDefinition>
					<Role>'+
						'<ID>' + t2.RoleID + '</ID>' +
						'<Name>' + t2.RoleNm +'</Name>'+		
						(
							select 
								t1.RoleMembe as "Name"
							from #role_membere t1
							where 1 = 1
							and t1.RoleNm = t2.RoleNm
							for xml raw('Member') ,elements,root ('Members')
						) + 
					'</Role>
				</ObjectDefinition>'+
			'</Alter>'
			as xml
		)
from #role_membere t2
where 1 = 1
group by 
t2.RoleNm,
t2.RoleID,
t2.SSAS_DB
for xml raw('') ,elements,root ('Batch')

--Построчно
select 
	CAST(
			'<Alter ObjectExpansion="ExpandFull" xmlns="http://schemas.microsoft.com/analysisservices/2003/engine">' + 
				'<Object>
					<RoleID>' +  t2.RoleID + '</RoleID>
					<DatabaseID>' + t2.SSAS_DB + '</DatabaseID>
				</Object>'+
				'<ObjectDefinition>
					<Role>'+
						'<ID>' + t2.RoleID + '</ID>' +
						'<Name>' + t2.RoleNm +'</Name>'+		
						(
							select 
								t1.RoleMembe as "Name"
							from #role_membere t1
							where 1 = 1
							and t1.RoleNm = t2.RoleNm
							for xml raw('Member') ,elements,root ('Members')
						) + 
					'</Role>
				</ObjectDefinition>'+
			'</Alter>'
			as xml
		)
from #role_membere t2
where 1 = 1
group by 
t2.RoleNm,
t2.RoleID,
t2.SSAS_DB

