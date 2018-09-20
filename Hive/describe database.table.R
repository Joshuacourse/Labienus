library(jpeg)


img <-readJPEG("describe database.table.jpg")
grid::grid.raster(img)

### 如果我们先用 use database，则可以这样写

img2 <-readJPEG("describe table.jpg")
grid::grid.raster(img2)


