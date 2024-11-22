import pandas as pd
import plotly.express as px
import plotly.graph_objs as go

# Read the CSV file with explicit parsing
df = pd.read_csv('CO2sensors_.csv', 
                 delimiter=';', 
                 parse_dates=['Server time'],
                 decimal='.',  # Ensure decimal parsing
                 thousands=None)

# Ensure numeric conversion for columns
df['CO2 concentration'] = pd.to_numeric(df['CO2 concentration'], errors='coerce')
df['Temperature'] = pd.to_numeric(df['Temperature'], errors='coerce')
df['Humidity'] = pd.to_numeric(df['Humidity'], errors='coerce')

# Line Chart for CO2, Temperature, and Humidity
fig1 = go.Figure()
fig1.add_trace(go.Scatter(x=df['Server time'], y=df['CO2 concentration'], 
                           name='CO2 Concentration', yaxis='y1', line=dict(color='blue')))
fig1.add_trace(go.Scatter(x=df['Server time'], y=df['Temperature'], 
                           name='Temperature', yaxis='y2', line=dict(color='red')))

# Update layout with two y-axes
fig1.update_layout(
    title='CO2 Concentration and Temperature Over Time',
    yaxis=dict(title='CO2 Concentration (ppm)'),
    yaxis2=dict(title='Temperature (°C)', overlaying='y', side='right')
)
fig1.write_html('plots/line_chart.html')

# Bar Chart for Sensor Readings
fig2 = go.Figure(data=[
    go.Bar(name='CO2 Concentration', x=df['Server time'], y=df['CO2 concentration'], marker_color='blue'),
    go.Bar(name='Temperature', x=df['Server time'], y=df['Temperature'], marker_color='red')
])
fig2.update_layout(
    title='Comparative Bar Chart of CO2 and Temperature', 
    barmode='group'
)
fig2.write_html('plots/bar_chart.html')

# Scatter Plot with Color Gradient for Humidity
fig3 = px.scatter(df, x='Temperature', y='CO2 concentration', 
                  color='Humidity', 
                  title='CO2 Concentration vs Temperature (Colored by Humidity)',
                  labels={'Humidity': 'Humidity (%)'})
fig3.write_html('plots/scatter_plot.html')

# Pie Chart for Relative Proportions (demonstrative)
fig4 = go.Figure(data=[go.Pie(
    labels=['CO2', 'Temperature', 'Humidity'], 
    values=[
        df['CO2 concentration'].mean(), 
        df['Temperature'].mean(), 
        df['Humidity'].mean()
    ]
)])
fig4.update_layout(title='Average Sensor Readings Proportion')
fig4.write_html('plots/pie_chart.html')

print("Charts have been generated successfully!")
print(df)  # Print dataframe to verify parsing