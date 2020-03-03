INSERT OVERWRITE DIRECTORY '#outputPath#'
STORED AS PARQUET
select
    *
from
    #dbTable#
#conditionClause#