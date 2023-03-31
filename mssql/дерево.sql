select 
        c4.KeyField, 
        c1.KeyField, 
        c2.KeyField, 
        c3.KeyField, 
        COALESCE(c1.KeyField, c2.KeyField, c3.KeyField) as main_keyfield 
FROM Contr c4 
LEFT JOIN Contr as c3 ON c3.KeyField = IsNull(c4.[ParrentKey], c4.AssignmentFromContract_KeyField) 
LEFT JOIN Contr as c2 ON c2.KeyField = IsNull(c3.[ParrentKey], c3.AssignmentFromContract_KeyField) 
LEFT JOIN Contr as c1 ON c1.KeyField = IsNull(c2.[ParrentKey], c2.AssignmentFromContract_KeyField) 
order by c1.KeyField 

/*************************************** 2 вариант *****************************************/

declare @i int

set @i=1

declare @sql varchar(max)

declare @leftJoin varchar(max)

set @leftJoin=''

declare @check int

set @check=1

declare @tblCheck table (chek int)

declare @COALESCE as varchar(max)

set @COALESCE=''

declare @KeyField as varchar (max)

set @KeyField=''

 

while @check>0

       begin

             set @leftJoin=@leftJoin + char(10)+' LEFT JOIN Contr as c'+cast(@i+1 as varchar)+' ON c'+cast(@i+1 as varchar)+'.KeyField = IsNull(c'+cast(@i as varchar)+'.[ParrentKey], c'+cast(@i as varchar)+'.AssignmentFromContract_KeyField)'

             set @sql='select count(*) FROM Contr c1 ' + @leftJoin + ' where c'+cast(@i+1 as varchar)+'.[ParrentKey] is not null or  c'+cast(@i+1 as varchar)+'.AssignmentFromContract_KeyField is not null'

             insert into @tblCheck(chek) execute(@sql)

             select @check=chek from @tblCheck

             set @i=@i+1

       end

 

 

while @i>1

       begin

             set @KeyField=@KeyField+char (10)+',c'+cast(@i as varchar)+'.KeyField'

             set @COALESCE=@COALESCE+','+'c'+cast(@i as varchar)+'.KeyField'

             set @i=@i-1

       end

 

set @COALESCE=Substring(@COALESCE,2 ,len(@COALESCE)-1)

set @COALESCE=',COALESCE(' + @COALESCE+') as main_keyfield '

set @KeyField=Substring(@KeyField,2 ,len(@KeyField)-1)

set @KeyField='c1.KeyField' + char(10)+@KeyField

set @sql='select ' + char(10)+@KeyField+char (10) + @COALESCE + char(10)+ 'FROM Contr c1 '+@leftJoin

print @sql

exec (@sql)