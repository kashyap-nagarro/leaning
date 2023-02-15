###### Required Libraries ######
import os
import pandas as pd  # pip install pandas openpyxl
import plotly.express as px  # pip install plotly-express
import streamlit as st  # pip install streamlit
#import plotly.graph_objects as go

from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import avg, sum, col,lit
import streamlit as st
import pandas as pd

# emojis: https://www.webfx.com/tools/emoji-cheat-sheet/

############# Page Config ############
st.set_page_config(page_title="Sales Overview", page_icon=":bar_chart:", layout="wide")
# logo_url= "data:image/jpeg;base64,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"
# st.image(logo_url)
st.write('<style>div.block-container{padding-top:1rem;}</style>', unsafe_allow_html=True)

########## Load the Data ##############
#@st.cache
# Create Session object
def create_session_object():
    connection_parameters = {
      "account": 'tb12678.central-india.azure',
      "user": 'kashyaps75',
      "password": 'Root_123',
      "warehouse": "compute_wh",
      "database": 'TESTDB_MG'
   }
    session = Session.builder.configs(connection_parameters).create()
    print(session.sql('select current_warehouse(), current_database(), current_schema()').collect())
    return session

session=create_session_object()

def load_data(session):
    # CO2 Emissions by Country
    snow_df = session.table("TESTSCHEMA_MG.SALES")
    snow_df1  = snow_df.to_pandas()
    return snow_df1
    
df=load_data(session)
df["hour"] = pd.to_datetime(df["Time"], format="%H:%M:%S").dt.hour
df["Date"] = pd.to_datetime(df["Date"], format="%d-%m-%Y")

min_date=df['Date'].min()
max_date=df['Date'].max()
# print(min_date,max_date)
####### Create SideBar ##############
st.sidebar.header("Please Filter Here")

Dimention=st.sidebar.selectbox("Please Select the Dimension", 
                                options=['Product_line','Branch', 'City', 
                                        'Customer_type', 'Gender',
                                        'Payment_Method','hour'])

measure=st.sidebar.selectbox("Please Select the Measure", 
                                options=['Sales Amount','COGS', 'Margin', 
                                        'Quantity'])
           
city=st.sidebar.multiselect("Select the City", 
                            options=df["City"].unique(),
                            default=df["City"].unique())

branch=st.sidebar.multiselect("Select the Branch", 
                                     options=df["Branch"].unique(),
                            default=df["Branch"].unique())
                            
customer_type=st.sidebar.multiselect("Select the Customer Type", 
                                     options=df["Customer_type"].unique(),
                            default=df["Customer_type"].unique())

gender=st.sidebar.multiselect("Select the Gender", 
                                     options=df["Gender"].unique(),
                            default=df["Gender"].unique())




# ---- MAINPAGE ----
st.title(":bar_chart: Sales Overview")
st.markdown("##")

####### Add the Filter ############
Col1_date,col2_date=st.columns(2)
start_date = Col1_date.date_input("Start Date", value=pd.to_datetime(min_date, format="%d-%m-%Y"),
                                  min_value=pd.to_datetime(min_date, format="%d-%m-%Y"), max_value=pd.to_datetime(max_date, format="%d-%m-%Y"))
end_date = col2_date.date_input("End Date", value=pd.to_datetime(max_date, format="%d-%m-%Y"),
                                min_value=pd.to_datetime(min_date, format="%d-%m-%Y"), max_value=pd.to_datetime(max_date, format="%d-%m-%Y"))

df_selection=df.query("City==@city & Branch==@branch & \
                      Customer_type==@customer_type & \
                      Gender==@gender & Date >= @start_date \
                       and Date <=@end_date")

# TOP KPI's
total_sales = int(df_selection["Sales Amount"].sum())
average_rating = round(df_selection["Customer Rating"].mean(), 1)
star_rating = ":star:" * int(round(average_rating, 0))
average_sale_by_transaction = round(df_selection["Sales Amount"].mean(), 2)

left_column, middle_column, right_column = st.columns(3)
with left_column:
    st.subheader("Total Sales:")
    st.subheader(f"US $ {total_sales:,}")
with middle_column:
    st.subheader("Average Rating:")
    st.subheader(f"{average_rating} {star_rating}")
with right_column:
    st.subheader("Average Transaction Size:")
    st.subheader(f"US $ {average_sale_by_transaction}")

st.markdown("""---""")

####### Do the aggregation based on Dimension and Messure
Agg1=["count","sum","min","mean","max"]
agg_c={measure:Agg1}

messure_by_dimension1 = (
    df_selection.groupby(by=Dimention).agg(agg_c)
)
messure_by_dimension1.columns=messure_by_dimension1.columns.droplevel()

######## Create Bar Plot using Dimesion and Messure #####
fig_dimesion_messure1 = px.bar(
    messure_by_dimension1,
    x=messure_by_dimension1.index,
    y= "sum",
    # orientation="h",
    title=f"<b>{measure} by {Dimention}</b>",
    color_discrete_sequence=["#0083B8"] * len(messure_by_dimension1),
    template="plotly_white",
    labels=dict(y=measure)
)
fig_dimesion_messure1.update_layout(
    plot_bgcolor="rgba(0,0,0,0)",
    xaxis=(dict(showgrid=False)),
    yaxis_title=measure
)

# print("run")
######## Do the aggregation based on Dimension and Average Rating
Agg2=["min","mean","max"]
agg_c2={"Customer Rating":Agg2}

messure_by_dimension2= (
    df_selection.groupby(by=Dimention).agg(agg_c2)
)
messure_by_dimension2.columns=messure_by_dimension2.columns.droplevel()

######## Create Line Plot using Dimesion and Average Rating #####
fig_dimesion_messure2 = px.line(x=messure_by_dimension2.index, y=messure_by_dimension2["mean"], title=f"<b>Consumer Rating by {Dimention}</b>")
fig_dimesion_messure2.update_layout(
    yaxis_title="Average Customer Rating",
    xaxis_title=Dimention
)

###### Align the Bar and Line Plot ###########
left_column,right_coloumn =st.columns(2)
left_column.plotly_chart(fig_dimesion_messure1, use_container_width=True)
right_coloumn.plotly_chart(fig_dimesion_messure2, use_container_width=True)

# print("run")
st.markdown("""---""")

####### Create a Dropdow to chose a 2n dimension ##########
Dimention2=st.selectbox("Please Select the 2nd Dimension", 
                                options=['Branch', 'City', 
                                        'Customer_type', 'Gender',
                                        'Payment_Method','hour']
)


####### Do the aggregation based on Primary Dimension,2nd Dimension and Messure
agg_c3={measure:["sum"]}
messure_by_dimension2= (
    df_selection.groupby(by=["Product_line",Dimention2]).agg(agg_c3).reset_index()
)
messure_by_dimension2.columns=messure_by_dimension2.columns.droplevel()
messure_by_dimension2.columns=["Product_line",Dimention2,"sum"]

######## Create Stack Bar Plot using Primary Dimension,2nd Dimension and Messure #####
fig_dimesion_messure3 = px.bar(messure_by_dimension2, x="Product_line", y="sum", color=Dimention2, 
                               text_auto='.2s',title=f"<b>{measure} by Product_line vs {Dimention2}</b>")

fig_dimesion_messure3.update_layout(
    #plot_bgcolor="rgba(0,0,0,0)",
    #xaxis=(dict(showgrid=False)),
    yaxis_title=measure
)

st.plotly_chart(fig_dimesion_messure3)
# print("run")
###### Create Pie Chart based on Transaction count and Dimesion 
fig_dimesion1 = px.pie(messure_by_dimension1, values="count", 
                             names=messure_by_dimension1.index,
                             hole=.3,
                             #labels={"Payment":"Mode of Payment"},
                             title=f"Transaction Count by {Dimention}",
                             template="presentation")
st.plotly_chart(fig_dimesion1)


# df_sle2=df_selection.groupby([Dimention,Dimention2,"Gender"]).sum()[[measure]].reset_index()
# fig1 = px.sunburst(df_sle2,
#                     path=[Dimention,Dimention2,"Gender"],
#                     values=measure,
#                     # color=cols[1],
#                 )
# fig1.update_traces(textinfo="label+percent entry")
# # fig1.update_layout(
# #                         title=f"{value} Distribution",
# #                         xaxis_title="Title",
# #         #                 # yaxis_title=yaxis,
# #         #                 legend_title="Dist. plot",
# #                         )
# st.plotly_chart(fig1)
# print("run")
# st.checkbox("Please select")
# print("run")
# ---- HIDE STREAMLIT STYLE ----
hide_st_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            header {visibility: hidden;}
            </style>
            """
st.markdown(hide_st_style, unsafe_allow_html=True)

print("Helo ML and DL")
