## Code of conduct

We must respect [Go Community code of conduct](https://golang.org/conduct)

## Vendoring

Think twice before add new dependency to the project - do we really need it?

We do vendoring in this project via [dep](https://github.com/golang/dep). 
All files in `vendor` folder must be under CVS, so don't forget to execute `dep ensure` before commit.