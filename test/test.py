import pandas as pd

# Create two DataFrames with the same columns
df1 = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
df2 = pd.DataFrame({"A": [5, 6], "B": [7, 8]})

# Append df2 to df1
result = df1.append(df2)

# Display the result
print(result)
