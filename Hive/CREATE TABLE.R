# The DROP DATABASE statement is used to drop an existing SQL database.
# Normally, a table is moved into the recycle bin (as of Oracle 10g), if it is dropped. However, if the purge modifier is specified as well, the table is unrecoverably (entirely) dropped from the database.
library(png)
img <- readPNG("CREATE TABLE.png")
grid::grid.raster(img)