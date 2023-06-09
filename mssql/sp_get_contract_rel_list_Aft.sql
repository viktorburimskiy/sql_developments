USE [Interleasing4]
GO
/****** Object:  UserDefinedFunction [dbo].[get_contract_rel_list_Aft]    Script Date: 18.12.2018 9:10:39 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


-- =============================================
-- Create date: 05.10.2017
-- Description:	Возвращает список дат из договора, где type not null (для расчета фактов в ИЛ4)
-- =============================================
ALTER FUNCTION [dbo].[get_contract_rel_list_Aft] 
(	
	@Contract_KeyField	BIGINT
)
RETURNS TABLE 
AS
RETURN 
(
	SELECT dt 
	FROM _cte_temp_Aft 
	WHERE DogovorKey =@Contract_KeyField  
		EXCEPT (
				SELECT  dt 
				FROM _cte_temp_Aft 
				WHERE ntype is null and DogovorKey =@Contract_KeyField
				)
)

