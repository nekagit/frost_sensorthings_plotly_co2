def debug_csv_read(file_path):
    # Read CSV with multi-level header
    df = pd.read_csv(file_path, sep=';', header=[0, 1], skiprows=[1])
    
    # Flatten column names
    df.columns = df.columns.get_level_values(0)
    
    # Convert Server time to datetime
    df['Server time'] = pd.to_datetime(df['Server time'], format='%Y-%m-%d %H:%M:%S')
    
    print("DataFrame Columns:", list(df.columns))
    print("\nDataFrame Head:")
    print(df.head())
    print("\nDataFrame Info:")
    print(df.info())
