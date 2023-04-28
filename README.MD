# Desarquivo

Desarquivo é uma aplicação *cli* que permite extrair factos diários a partir de várias fontes de informação para uma base de dados `sqlite`.

Extratores disponíveis:

* O Jogo - Fonte: Arquivo.pt
* Record - Fonte: Arquivo.pt
* Público - Fonte: Arquivo.pt
* Rádio Comercial - Fonte: Arquivo.pt
* RTP - Fonte: Arquivo.pt
* The Movie Database - Fonte: Api TMDB


# Pré-requisitos:

* `python 3.11`
* [Poetry](https://python-poetry.org)

Para a utilização do extractor [The Movie Database](https://www.themoviedb.org) é necessária uma API Key deste serviço devendo ser exportada a variavel de ambiente abaixo indicada.

    export TMDB_KEY=
    

# Utilização:

1. Instalar dependências com `poetry install`
2. Correr a aplicação:
    
**Extração de factos dos dias de Dezembro para os anos 2003, 2004 e 2005.**
        
        poetry run desarquivo -m 12 -sy 2003 -ey 2005
        
**Extração de factos do dia 1 de Maio para os anos 2010 e 2011.**
        
        poetry run desarquivo -m 5 -d 1 -sy 2010 -ey 2011
        
**Extração de factos do dia 28 de Janeiro para o ano 2010 apenas o extractor do Público**
        
        poetry run desarquivo -m 1 -d 28 -sy 2010 -ey 2011 -e PublicoV1
        
Os extractores disponíveis correspondem aos nomes das classes no package `extractors`.

3. A base de dados, em ficheiro único, é produzida na pasta `./db_files`. Por omissão o processo é aditivo e mais factos são adicionados à base de dados a cada execução.

Base de dados produzidas pelo `desarquivo` são disponibilizadas nas releases deste projeto no Github.

# Contribuir:

Contribuições são mais que bem-vindas, tanto para melhorar a ferramenta como par adicionar extractores que tornem a produção das bases de dados mais rica e variada.

# Licença:

MIT License

Copyright (c) [2023] [Cristiano Lopes]

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

