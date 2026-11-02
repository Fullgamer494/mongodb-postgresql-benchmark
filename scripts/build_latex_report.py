"""Genera el análisis descriptivo del CSV y un proyecto LuaLaTeX autocontenido."""
from pathlib import Path
import csv,collections,statistics,math,sys,json,shutil
sys.path.insert(0,'/tmp/metrics-report-deps')
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
R=Path(__file__).resolve().parents[1];O=R/'output/reporte-latex';F=O/'graficas';T=O/'tablas'
for p in [O,F,T]:p.mkdir(exist_ok=True,parents=True)
rows=list(csv.DictReader((R/'results/results_unificado_162.csv').open()))
cols=[k for k in rows[0] if k not in ['source_dir','template_source_dir','data_origin']]
with (O/'datos.csv').open('w',newline='') as f:
 w=csv.DictWriter(f,fieldnames=cols,extrasaction='ignore');w.writeheader();w.writerows(rows)
P=['PROFILE_A_BASELINE','PROFILE_B_MEDIUM','PROFILE_C_SEVERE'];Q=['Q1','Q2','Q2_ALT'];E=['mongodb','postgresql'];labels={'mongodb':'MongoDB','postgresql':'PostgreSQL'}
def subset(q,e,p,c):return [r for r in rows if (r['query_id'],r['engine'],r['profile'],r['concurrency'])==(q,e,p,str(c))]
def usable(rs):return [r for r in rs if r['status'] in ('ok','spill')]
def med(rs,k):
 vals=[float(r[k]) for r in rs if r[k]!=''];return statistics.median(vals) if vals else None
def num(v,n=2):return 's/d' if v is None else f'{v:.{n}f}'
def esc(t):
 return ''.join({'\\':r'\textbackslash{}','&':r'\&','%':r'\%','$':r'\$','#':r'\#','_':r'\_','{':r'\{','}':r'\}','~':r'\textasciitilde{}','^':r'\textasciicircum{}','→':r'\(\rightarrow\)','×':r'\(\times\)'}.get(c,c) for c in str(t))
def table(headers,data,widths=None):
 n=len(headers);ws=widths or [1/n]*n;total=6.9*72.27-2*n*3
 spec=''.join(r'>{\raggedright\arraybackslash}p{'+f'{total*w/sum(ws):.2f}pt'+'}' for w in ws)
 header=' & '.join(r'\textbf{'+esc(x).replace('\n',r'\newline{}')+'}' for x in headers)+r' \\'
 s=[r'\begingroup\fontsize{9}{10.8}\selectfont\setlength{\tabcolsep}{3pt}',r'\setlength{\LTpre}{6pt}\setlength{\LTpost}{8pt}',r'\begin{longtable}{'+spec+'}',header,r'\endfirsthead',header,r'\endhead',r'\endfoot',r'\endlastfoot']
 s+=[' & '.join(esc(x).replace('\n',r'\newline{}') for x in row)+r' \\' for row in data];s += [r'\end{longtable}\endgroup'];return '\n'.join(s)
summary={}
for q in Q:
 data=[]
 for p in P:
  for c in [1,10,50]:
   for e in E:
    rs=subset(q,e,p,c);good=usable(rs);st=collections.Counter(r['status'] for r in rs);status=', '.join(f'{k} ({st[k]}/3)' for k in ['ok','spill','error','oom'] if st[k])
    data.append([labels[e],p[8],c,num(med(good,'throughput_qps'),3),num(None if med(good,'latency_p95_ms') is None else med(good,'latency_p95_ms')/1000),num(None if med(good,'latency_p99_ms') is None else med(good,'latency_p99_ms')/1000),num(med(good,'cpu_throttled_seconds'),1),num(med(good,'max_rss_mb'),1),status])
 (T/f'{q}.tex').write_text(table(['Motor','Perfil','Clientes','QPS','P95\n(s)','P99\n(s)','Throttling\n(s)','RSS máx.\n(MiB)','Estado'],data,[1.1,.38,.55,.52,.53,.53,.75,.65,1.1]))
plt.rcParams.update({'font.family':'Arial','font.size':12,'text.color':'black','axes.labelcolor':'black','xtick.color':'black','ytick.color':'black','figure.facecolor':'white'})
for metric,name,ylabel in [('latency_p99_ms','p99','P99 (s, escala logarítmica)'),('max_rss_mb','rss','RSS máximo (MiB)')]:
 fig,axs=plt.subplots(1,3,figsize=(10,3.6))
 for ax,q in zip(axs,Q):
  for ei,e in enumerate(E):
   for pi,p in enumerate(P):
    vals=[med(usable(subset(q,e,p,c)),metric) for c in [1,10,50]]
    vals=[math.nan if v is None else v/(1000 if metric.startswith('latency') else 1) for v in vals]
    ax.plot([0,1,2],vals,c='black' if ei==0 else '.5',ls=['-','--',':'][pi],marker=['o','s'][ei],markersize=4,label=f'{labels[e]} {"ABC"[pi]}')
  ax.set_title(q.replace('_',' '));ax.set_xticks([0,1,2],['1','10','50']);ax.set_xlabel('Clientes');ax.grid(alpha=.2)
  if metric.startswith('latency'):ax.set_yscale('log')
 axs[0].set_ylabel(ylabel);h,l=axs[0].get_legend_handles_labels();fig.legend(h,l,loc='lower center',ncol=3,fontsize=10,frameon=False);fig.tight_layout(rect=(0,.16,1,1));fig.savefig(F/f'{name}.png',dpi=230,bbox_inches='tight');plt.close(fig)
fig,axs=plt.subplots(1,3,figsize=(10,3.6))
for ax,q in zip(axs,Q):
 for ei,e in enumerate(E):
  for pi,p in enumerate(P):
   rs=[r for r in rows if r['query_id']==q and r['engine']==e and r['profile']==p and r['status'] in ['ok','spill'] and r['cpu_throttled_seconds'] and r['throughput_qps']]
   ax.scatter([float(r['cpu_throttled_seconds']) for r in rs],[float(r['throughput_qps']) for r in rs],marker=['o','s','^'][pi],facecolors='none' if ei==0 else '.5',edgecolors='black',s=28,label=f'{labels[e]} {"ABC"[pi]}')
 ax.set_title(q.replace('_',' '));ax.set_xlabel('Throttling acumulado (s)');ax.grid(alpha=.2)
axs[0].set_ylabel('QPS');h,l=axs[0].get_legend_handles_labels();fig.legend(h,l,loc='lower center',ncol=3,fontsize=10,frameon=False);fig.tight_layout(rect=(0,.16,1,1));fig.savefig(F/'throttling.png',dpi=230,bbox_inches='tight');plt.close(fig)
fig,ax=plt.subplots(figsize=(9,3.3));cats=[(q,e) for q in Q for e in E];bottom=[0]*6
for status,col,hatch in [('ok','white',''),('spill','.8','///'),('error','.5','xx'),('oom','black','')]:
 counts=[sum(r['status']==status and r['query_id']==q and r['engine']==e for r in rows) for q,e in cats]
 ax.bar(range(6),counts,bottom=bottom,label=status,color=col,edgecolor='black',hatch=hatch,width=.6);bottom=[a+b for a,b in zip(bottom,counts)]
ax.set_xticks(range(6),[q.replace('_',' ')+'\n'+labels[e] for q,e in cats],fontsize=10);ax.set_ylabel('Registros');ax.set_ylim(0,30);ax.legend(ncol=4,frameon=False,loc='upper center',bbox_to_anchor=(.5,1.2));fig.tight_layout();fig.savefig(F/'estados.png',dpi=230,bbox_inches='tight');plt.close(fig)
# Una tabla de memoria con el denominador nominal de 500 000 eventos del playbook.
memory=[]
for q in Q:
 for e in E:
  r=usable(subset(q,e,P[0],1));v=med(r,'max_rss_mb');memory.append([q.replace('_',' '),labels[e],num(v),num(None if v is None else v/500,4)])
(T/'memoria.tex').write_text(table(['Consulta','Motor','RSS mediano máximo\n(MiB)','MiB por 1000 eventos\ndel dataset nominal'],memory,[1,1.2,1.5,2]))
# Valores que se citan en el texto se calculan a partir del mismo agrupamiento.
statuses=dict(collections.Counter(r['status'] for r in rows));print('Estados',statuses)
tex=r'''\documentclass[11pt,letterpaper]{article}
\usepackage[margin=0.8in,top=0.75in,bottom=0.75in]{geometry}
\usepackage{fontspec}
\setmainfont{Arial}
\usepackage[spanish,es-nodecimaldot]{babel}
\usepackage{setspace,titlesec,graphicx,array,longtable,caption,float,microtype}
\usepackage{hyperref}
\hypersetup{hidelinks,pdftitle={Análisis de métricas de agregaciones JSON},pdfauthor={}}
\usepackage{xurl}
\urlstyle{same}
\onehalfspacing
\setcounter{secnumdepth}{0}
\setcounter{tocdepth}{2}
\titleformat{\section}{\fontsize{14}{16.8}\selectfont\bfseries}{}{0pt}{}
\titleformat{\subsection}{\fontsize{12}{14.4}\selectfont\bfseries}{}{0pt}{}
\titlespacing*{\section}{0pt}{12pt}{8pt}
\titlespacing*{\subsection}{0pt}{10pt}{5pt}
\setlength{\parindent}{0pt}
\setlength{\parskip}{7pt}
\setlength{\emergencystretch}{3em}
\widowpenalty=10000\clubpenalty=10000
\captionsetup{font=small,labelfont=normalfont,justification=justified,singlelinecheck=false}
\pagestyle{plain}
\begin{document}
{\fontsize{16}{24}\selectfont\bfseries Análisis de métricas de agregaciones JSON\par}
MongoDB y PostgreSQL JSONB bajo límites de CPU y memoria\par
Bases de Datos en la Nube\par
{\setstretch{1.05}\tableofcontents}
\clearpage
\section{Introducción}
Los eventos de seguridad de una red reúnen información del dispositivo, su ubicación, el incidente y las detecciones asociadas. Cuando estos datos se almacenan como documentos anidados, una consulta puede necesitar recorrer arreglos completos, calcular expresiones y agrupar resultados. Otras consultas seleccionan los incidentes de mayor riesgo mediante filtros y ordenamientos. Aunque ambas operaciones leen el mismo dominio, su costo puede ser distinto.

El propósito de este reporte es describir los valores de latencia, rendimiento y memoria incluidos en el CSV unificado. La comparación distingue MongoDB y PostgreSQL JSONB, los perfiles de recursos A, B y C, y las concurrencias de 1, 10 y 50 clientes. Q1 representa el trabajo de desanidar y agrupar detecciones; Q2 y Q2\_ALT corresponden a la selección Top-1000 con dos tratamientos de memoria.

La lectura conjunta de throughput, percentiles y estados permite identificar qué combinaciones ofrecen más respuestas y cuáles registran errores o agotamiento de memoria. El análisis sigue los apartados del playbook y responde sus preguntas M1, M2 y M3. Sus conclusiones se refieren a los valores del archivo y al alcance de las métricas que contiene.

\section{Especificaciones del entorno y perfiles de recursos}
La clasificación del playbook asigna cuatro CPU y 4096 MiB al perfil A, dos CPU y 2048 MiB al B, y una CPU y 1024 MiB al C. Estos presupuestos permiten organizar la comparación desde la mayor disponibilidad hasta la contención severa. La columna profile identifica cada grupo y la columna engine distingue los dos motores.

\begin{center}
\begin{tabular}{llll}
\textbf{Perfil} & \textbf{CPU} & \textbf{RAM} & \textbf{Clasificación}\\
A & 4 & 4096 MiB & Baseline\\
B & 2 & 2048 MiB & Contención media\\
C & 1 & 1024 MiB & Contención severa\\
\end{tabular}
\end{center}

El CSV no incluye un inventario del procesador, versiones de software o parámetros efectivos de Docker. Por ello, los perfiles se emplean como categorías de comparación. La cuota de CPU y la RAM cambian juntas entre perfiles; el análisis no permite aislar el efecto marginal de cada recurso.

\clearpage
\section{Método de análisis y comparación con el baseline}
\subsection{Consultas y estructura de la matriz}
Q1 desanida detections, filtra impact\_score mayor que 50 y agrupa por país y categoría. La expresión occurrences multiplicada por impact\_score permite obtener sumas y promedios, además del conteo de detecciones. Q2 selecciona incidentes confirmados desde una fecha, ordena risk\_score en forma descendente y limita la salida a mil registros. El tratamiento base utiliza allowDiskUse=false en MongoDB y work\_mem=4MB en PostgreSQL; Q2\_ALT utiliza true y 64MB, respectivamente, según la definición de las cargas del proyecto.

El archivo contiene 162 registros: dos motores, tres perfiles, tres consultas o variantes, tres concurrencias y tres valores de run\_number. La agrupación por motor, perfil, consulta y concurrencia produce 54 combinaciones, cada una con tres registros. Este diseño permite comparar perfiles conservando consulta y número de clientes.

\subsection{Lectura de las métricas y tratamiento de valores vacíos}
Throughput se expresa en consultas completas por segundo. P50 es la mediana de latencias y P95 y P99 describen el extremo lento de la distribución: un P99 elevado implica que una parte de las respuestas tarda mucho más que la respuesta típica. En las tablas se presentan P95 y P99 en segundos; las columnas del archivo están en milisegundos y se dividieron entre mil.

Cada tabla muestra la mediana por combinación. Para resumir rendimiento se consideran los registros con estado ok o spill y un valor disponible en la métrica. Un registro spill conserva valor analítico porque indica que la operación finalizó con almacenamiento temporal. Los estados error y oom se cuentan y se comentan por separado. Así, una combinación sin respuestas admitidas para el resumen aparece como s/d y conserva su estado adverso.

Hay 15 campos vacíos de throughput y 18 de P95 y P99. Se mantienen como datos ausentes; no se convierten en cero. Las medianas de percentiles son resúmenes de los valores por registro y no equivalen a recalcular un percentil sobre todas las solicitudes individuales. La columna Estado de las tablas indica cuántos de los tres registros tienen cada estado.

La variable cpu\_throttled\_seconds representa tiempo acumulado de restricción de CPU. No equivale a CPU consumida ni a segundos adicionales de duración de una consulta. max\_rss\_mb se interpreta en MiB conforme al contrato del laboratorio y describe el pico de memoria residente. disk\_spill indica almacenamiento temporal cuando contiene true o false; sus 22 vacíos se conservan como desconocidos.

\clearpage
\section{Resultados consolidados}
La distribución de estados es de 119 ok, 21 spill, 19 error y 3 oom. Los registros ok y spill representan 140 de 162 filas, equivalentes al 86.42\%. Los errores y OOM suman 22 filas, un 13.58\%. Estos porcentajes resumen el archivo y no sustituyen la evaluación de cada combinación.

\subsection{Q1 Desanidar filtrar y agrupar}
\input{tablas/Q1.tex}
Con un cliente, MongoDB presenta QPS medianos de 0.503, 0.530 y 0.463 en los perfiles A, B y C. PostgreSQL registra 0.407, 0.450 y 0.420. Esta concurrencia no sigue una caída estrictamente monotónica entre A y B, de modo que no basta para atribuir cada diferencia a la cuota de recursos.

La separación se vuelve mayor con diez clientes. MongoDB pasa de 1.163 QPS en A a 0.200 en C; PostgreSQL, de 0.920 a 0.157. Con cincuenta clientes, el perfil A conserva 1.167 y 0.857 QPS, pero B registra error en ambos motores y C registra error en MongoDB y oom en PostgreSQL. Las 21 filas spill corresponden a PostgreSQL Q1. En esta carga, una ventaja en throughput debe leerse junto con la latencia y los estados de terminación.

\clearpage
\subsection{Q2 Ordenamiento con tratamiento base}
\input{tablas/Q2.tex}
PostgreSQL registra mayor throughput mediano en todas las combinaciones de Q2 con resumen disponible para ambos motores. Con un cliente en A, la diferencia es de 2.840 frente a 0.193 QPS, aproximadamente 14.69 veces. P99 es de 0.40 s en PostgreSQL y 6.78 s en MongoDB.

En C con diez clientes, PostgreSQL conserva 1.073 QPS y MongoDB 0.233, una relación de 4.60 veces. Sus P99 son 10.69 y 44.50 s. Para cincuenta clientes en ese perfil, MongoDB registra error en los tres renglones, mientras PostgreSQL tiene valores disponibles y P99 mediano de 37.99 s.

En A y B, el resumen de Q2 coincide para diez y cincuenta clientes en varias métricas. Esta igualdad debe leerse como una propiedad del archivo; por sí sola no demuestra capacidad adicional ni una ley de escalabilidad. El aumento de clientes no puede interpretarse como mejora si QPS y latencia no cambian.

\clearpage
\subsection{Q2 ALT Ordenamiento con tratamiento alternativo}
\input{tablas/Q2_ALT.tex}
En A con diez clientes, PostgreSQL alcanza 5.473 QPS y MongoDB 0.833. Frente a Q2 base en esa combinación, el incremento de PostgreSQL es de aproximadamente 0.80\%, mientras MongoDB queda 3.85\% por debajo. La ampliación del tratamiento no produce una mejora uniforme.

Con diez clientes en B, PostgreSQL pasa de 1.867 QPS en Q2 a 2.263 en Q2\_ALT, cerca de 21.25\% más; MongoDB pasa de 0.367 a 0.500, un 36.36\% más. En C con un cliente ocurre lo contrario: Q2\_ALT tiene menor throughput que Q2 en ambos motores. Los cambios dependen de la combinación y no justifican elevar memoria de forma general.

MongoDB Q2\_ALT en A con cincuenta clientes tiene dos registros ok y uno error. Su mediana de rendimiento usa únicamente los dos valores ok, por lo que no debe confundirse con una combinación sin fallos. En los perfiles B y C con cincuenta clientes, los tres registros de MongoDB tienen estado error.

\clearpage
\section{Análisis gráfico}
\subsection{P99 frente a concurrencia}
\begin{figure}[H]\centering
\includegraphics[width=\linewidth]{graficas/p99.png}
\caption{P99 mediano por consulta y concurrencia. La escala vertical es logarítmica. Los círculos corresponden a MongoDB y los cuadrados a PostgreSQL; el trazo distingue A, B y C. Los huecos representan combinaciones sin valores admitidos para el resumen.}
\end{figure}
En Q1, pasar de uno a diez clientes en el perfil C eleva P99 de MongoDB de 2.49 a 63.19 s y el de PostgreSQL de 2.48 a 67.26 s. La cola de latencia crece mucho más que la concurrencia, señal de que más solicitudes comparten un presupuesto insuficiente para mantener la respuesta de un solo cliente.

Los paneles de Q2 y Q2\_ALT colocan a PostgreSQL por debajo de MongoDB en latencia en gran parte de las combinaciones disponibles. Las interrupciones de las curvas son relevantes: los estados error y oom no se dibujan como latencias nulas ni se completan con interpolación.

\subsection{Throttling frente a throughput}
\begin{figure}[H]\centering
\includegraphics[width=\linewidth]{graficas/throttling.png}
\caption{Relación entre throttling acumulado y QPS en registros ok o spill. La forma representa el perfil y el relleno distingue el motor. Cada punto utiliza los dos valores del mismo renglón.}
\end{figure}
El gráfico permite ubicar grupos con restricción de CPU y comparar su rendimiento. En Q1, el perfil C con diez clientes concentra más throttling y menor QPS que A. Esta asociación no permite aislar una causa: también cambian la cuota de memoria y la cantidad de trabajo compartido.

El contador de throttling puede superar los 300 s nominales de una ventana. No debe interpretarse como si la consulta hubiera permanecido detenida ese número de segundos de reloj. Su utilidad aquí es comparar los valores bajo el mismo contrato, sin confundirlos con consumo de CPU.

\clearpage
\subsection{RSS máximo frente a concurrencia}
\begin{figure}[H]\centering
\includegraphics[width=\linewidth]{graficas/rss.png}
\caption{Mediana de RSS máximo por consulta, motor, perfil y concurrencia. La curva resume picos por registro; no representa la evolución temporal de RSS dentro de una ventana.}
\end{figure}
En Q1 y perfil A, PostgreSQL pasa de 42.21 MiB con un cliente a 1646.30 MiB con cincuenta. MongoDB pasa de 530.90 a 749.05 MiB. El menor nivel inicial de PostgreSQL no se mantiene como menor crecimiento al aumentar la concurrencia en esa carga.

Q2 mantiene una separación distinta. PostgreSQL presenta valores residentes mucho menores en los grupos resumidos, mientras MongoDB se ubica en torno a varios cientos de MiB. RSS expresa ocupación residente; por sí solo no informa cuántas consultas se completaron ni qué parte de la memoria corresponde a estructuras internas reutilizables.

El CSV contiene máximos por registro, no muestras temporales de memoria. Por esa razón se representa RSS frente a concurrencia, sin construir una curva ficticia de uso de RAM segundo a segundo.

\subsection{Estados por consulta y motor}
\begin{figure}[H]\centering
\includegraphics[width=\linewidth]{graficas/estados.png}
\caption{Conteo de estados en cada grupo de consulta y motor. Cada barra reúne 27 registros. Los estados adversos se mantienen visibles aunque no contribuyan a las medianas de rendimiento.}
\end{figure}
La distribución permite distinguir un valor favorable de rendimiento de una combinación estable. En Q1, PostgreSQL reúne todos los spill y los tres oom. En las dos variantes de Q2, PostgreSQL figura con estado ok en los 54 registros. Los errores de MongoDB se concentran en las combinaciones de mayor concurrencia y no deben ocultarse al comparar QPS.

\clearpage
\section{Planes de ejecución e interpretación de las consultas}
El playbook solicita contrastar executionStats de MongoDB con EXPLAIN ANALYZE BUFFERS de PostgreSQL. Las columnas del CSV describen resultados agregados y no contienen operadores, estimaciones de cardinalidad, documentos examinados, bloques temporales ni métodos de ordenamiento. Por ello, este análisis no presenta un plan específico como si pudiera deducirse de las latencias.

La estructura de Q1 implica desanidar, filtrar y agrupar, mientras Q2 combina selección, ordenamiento y límite. Esa diferencia de trabajo ayuda a formular hipótesis para leer las tablas. El spill de PostgreSQL en Q1 es compatible con uso de almacenamiento temporal en ordenamiento o agrupación, pero el booleano no permite identificar qué operador lo produjo ni cuántos bytes escribió.

Tampoco se deduce del CSV si Q2 usa un índice, un recorrido secuencial o un ordenamiento Top-N. La diferencia de throughput entre motores es descriptiva; atribuirla a un operador exige el plan correspondiente. Esta distinción evita explicar una cifra con un mecanismo que el archivo no registra.

\section{Interpretación de memoria disco y throttling}
\subsection{M1 Cambios en P95 y P99 al limitar CPU y memoria}
La comparación de Q1 con diez clientes muestra un efecto marcado entre perfiles. En MongoDB, P95 pasa de 10.10 s en A a 59.71 s en C y P99 de 10.78 a 63.19 s. En PostgreSQL, P95 aumenta de 12.06 a 65.20 s y P99 de 12.76 a 67.26 s. El cambio es de aproximadamente 5.86 veces en P99 de MongoDB y 5.27 veces en PostgreSQL.

Este aumento coincide con una caída del throughput y con mayores valores de throttling. Los perfiles reducen CPU y memoria de forma conjunta, por lo que no se separa cuánto corresponde a cada límite. Con cincuenta clientes, los estados adversos de B y C también forman parte de la respuesta a M1: no basta comparar percentiles entre las combinaciones que conservan valores.

Q2 muestra una ventaja descriptiva de PostgreSQL en las celdas con datos para ambos motores. En perfil C y diez clientes, su P99 mediano es aproximadamente 4.16 veces menor que el de MongoDB. El tamaño de la diferencia cambia por consulta, perfil y concurrencia; no existe un factor único de degradación para todo el archivo.

\subsection{M2 Buffers antes de spill u OOM}
En Q1, las 21 filas spill de PostgreSQL corresponden a combinaciones que conservan un resumen de rendimiento; los tres registros oom se ubican en perfil C con cincuenta clientes. MongoDB no registra spill en Q1, aunque sí error en B y C con cincuenta clientes. Para esta consulta, la tabla permite distinguir dos comportamientos: uso de disco temporal en PostgreSQL y ausencia de esa marca en MongoDB, junto con fallos en los extremos de recursos.

Esto no basta para afirmar qué motor administra internamente sus buffers con mayor eficiencia en todos los casos. El CSV no contiene ocupación de buffers de ordenamiento, bytes temporales ni la causa de cada error. La columna status=error tampoco identifica un timeout, una desconexión o un error distinto. La interpretación debe conservar esas diferencias.

En Q2 y Q2\_ALT, PostgreSQL tiene estado ok en todas las filas y no registra spill. MongoDB tampoco presenta un estado spill, pero acumula errores en varias combinaciones. El tratamiento alternativo tiene mejoras de throughput en B con diez clientes y retrocesos en otros grupos. Por tanto, permitir disco o elevar work\_mem no se traduce automáticamente en una mejor respuesta.

\subsection{M3 Huella de CPU y memoria por mil documentos}
La CPU por mil documentos requiere dos magnitudes: segundos de CPU consumidos y documentos realmente procesados. Ninguna aparece en el CSV. El tiempo de throttling no sustituye a CPU consumida y el throughput cuenta consultas, no documentos. Con estas columnas no es posible calcular una tasa válida de CPU por mil documentos ni responder cuál motor consume menos CPU por unidad de trabajo.

La memoria sí puede describirse mediante RSS máximo. Con el tamaño nominal de 500 000 eventos definido por la guía, dividir RSS entre 500 produce una normalización por mil eventos del dataset. Esta medida no representa memoria incremental de cada consulta ni memoria por mil documentos examinados. La tabla utiliza el perfil A con un cliente y la mediana de los registros ok o spill.

\input{tablas/memoria.tex}

En Q1, la normalización es de 1.0618 MiB por mil eventos en MongoDB y 0.0844 en PostgreSQL. En Q2 es de 1.1012 y 0.0172, respectivamente. Esta diferencia describe la ocupación residente relativa del conjunto nominal de datos. Debe interpretarse junto con throughput, spill y concurrencia: una menor memoria inicial no garantiza una respuesta más rápida ni menor crecimiento con más clientes.

\clearpage
\section{Recomendaciones técnicas para despliegues documentales en la nube}
Para una carga centrada en Q1, MongoDB presenta mayor throughput que PostgreSQL en las combinaciones con resumen disponible para ambos. El perfil A conserva valores con cincuenta clientes, mientras B y C registran estados adversos en esa concurrencia. Una decisión de capacidad debería priorizar el P99 y el porcentaje de errores, además de las consultas por segundo.

PostgreSQL presenta una ventaja descriptiva en Q2 y Q2\_ALT: conserva valores de rendimiento y estado ok en todas sus combinaciones de esas variantes. Esto lo vuelve una opción a considerar para recuperación Top-1000 sobre JSONB dentro de las condiciones representadas por el archivo. La conclusión depende de la consulta y de los parámetros; no sustituye una evaluación de otras cargas documentales.

El tratamiento alternativo merece una decisión por escenario. En B con diez clientes mejora QPS en ambos motores, pero ese patrón no se mantiene en todos los perfiles. Elevar work\_mem para todas las conexiones puede aumentar la demanda conjunta de memoria; habilitar allowDiskUse tampoco obliga al motor a escribir temporales. Conviene evaluar cada ajuste junto con sus percentiles, RSS y estados, en lugar de asumir que más presupuesto interno mejora cualquier consulta.

Para dimensionar un servicio se recomienda fijar un objetivo de latencia y un máximo admisible de errores antes de elegir el tamaño de la instancia. Una combinación que eleva throughput a costa de una cola de más de un minuto puede ser inadecuada para una solicitud interactiva, aunque sirva para trabajo diferido. Los registros con valores vacíos deben conservarse como ausencia de la métrica y los estados adversos como parte del resultado.

El análisis muestra que la elección de motor cambia con la operación predominante. Q1 concentra el trabajo en desanidar y agrupar, mientras Q2 selecciona y ordena un subconjunto. La evaluación conjunta de rendimiento, cola de latencia y estabilidad ofrece una base más útil para decidir recursos que el promedio de todas las filas o una clasificación única de ganador.

\section{Referencias}
Universidad Politécnica de Chiapas. \emph{Medición de métricas en bases documentales}. Bases de Datos en la Nube, IT2iD, 2026. Guía playbook, apartados de métricas, perfiles, consultas y entregables.

Contrato de medición y definición de consultas del laboratorio de eventos de seguridad de red. Columnas de throughput, percentiles, throttling, RSS y estados.

\end{document}
'''
(O/'reporte.tex').write_text(tex)
(O/'compilar.sh').write_text('#!/usr/bin/env bash\nset -euo pipefail\ncd -- "$(dirname -- "$0")"\nlatexmk -lualatex -interaction=nonstopmode -halt-on-error reporte.tex\n')
(O/'compilar.sh').chmod(0o755)
(O/'README.md').write_text('''# Reporte LaTeX\n\nEl PDF se genera a partir de `reporte.tex`, las tablas de `tablas/` y las imágenes de `graficas/`.\n\n## Compilación\n\n```bash\n./compilar.sh\n```\n\nTambién puede ejecutarse `latexmk -lualatex -interaction=nonstopmode -halt-on-error reporte.tex`. Requiere LuaLaTeX, latexmk, Arial y los paquetes indicados en el preámbulo. El índice se actualiza durante la compilación. `datos.csv` contiene las columnas de métricas utilizadas para el análisis.\n\nEl generador `scripts/build_latex_report.py`, desde la raíz del proyecto, reconstruye tablas, imágenes y fuente a partir del CSV unificado. La compilación del documento no requiere Python.\n''')
print('Proyecto creado:',O)
