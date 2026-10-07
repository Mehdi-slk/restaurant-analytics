select distinct profile_key, inspection_type, inspection_action, inspection_grade
from {{ ref('int_inspection_lines') }}

union all
select '-1','Unknown','Unknown','Not Graded'