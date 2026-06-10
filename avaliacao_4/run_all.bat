@echo off

cmd /c start "Gateway" cmd /k python ms_gateway.py
cmd /c start "Notificacao" cmd /k python ms_notificacao.py
cmd /c start "Promocao" cmd /k python ms_promocao.py
cmd /c start "Ranking" cmd /k python ms_ranking.py
cmd /c start "Frontend" cmd /k python -m http.server 5500