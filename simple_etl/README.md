## Exercise: Simple ETL

1. Familiarize yourself with documentation of the https://dummyjson.com/ API.
2. Use the uv tool to setup your project. Use ruff for formatting. Use pyproject.toml
3. Get all users in batches (use pagination).
4. Process a batch before getting the next one.
5. Get first_name, last_name, a few more entries of your choice, and coordinates (latitude, longitude).
6. Get country name using the coordinates for all users.
7. Find the product category most often put into a basket by every user.
8. Save the results to file (e.g. csv, txt).
9. Save the results to a database, e.g. sqlite, and add a unique, monotonically increasing id.