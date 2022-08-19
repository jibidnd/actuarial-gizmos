import pandas as pd

from gzmo.core import Book
from gzmo.rating.rating_plan import RatingPlan, InterpolatedRatingTable

# initialize an emtpy rating plan
nj06 = RatingPlan.from_excel(r'examples/Metromile/NJ06.xlsx')

dag = nj06.make_dag()

# tuple(dag.static_order())
dag.prepare()
while dag.is_active():
    s = list(dag.get_ready())
    print(s)
    dag.done(*s)
    print('\n\n')
        
pd.DataFrame(columns = {i: None for x in nj06.values() for i in x.inputs}.keys()).to_clipboard()


# all_inputs = {i: None for x in nj06.values() for i in x.inputs}.keys()
# all_outputs = {i: None for x in nj06.values() for i in x.outputs}.keys()
# all_outputs

pd.DataFrame(columns = all_outputs).to_clipboard()