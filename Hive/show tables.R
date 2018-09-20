### show tables 是在指定了database以后
### 如果还没有指定，那么就show tables in

library(jpeg)
img <-readJPEG("show tables.jpg")
grid::grid.raster(img)

### 输出名字满足一定pattern的table
library(png)
img <- readPNG("show tables wildcard.png")
grid::grid.raster(img)
