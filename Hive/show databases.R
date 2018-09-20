library(jpeg)


img <-readJPEG("show databases.jpg")
grid::grid.raster(img)
