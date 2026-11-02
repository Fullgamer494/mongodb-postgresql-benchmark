"""Reporte reproducible: solo filas del unificado respaldadas por su escenario real."""
from pathlib import Path
import csv,json,hashlib,statistics,math,sys,collections,shutil
from decimal import Decimal
sys.path.insert(0,'/tmp/metrics-report-deps')
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from docx import Document
from docx.shared import Inches,Pt,RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH,WD_BREAK,WD_TAB_ALIGNMENT,WD_TAB_LEADER
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'output/reporte-metricas';OUT.mkdir(parents=True,exist_ok=True)
FIG=OUT/'graficas';FIG.mkdir(exist_ok=True)
SOURCE=ROOT/'results/results_unificado_162.csv'
SNAPSHOT=OUT/'datos_fuente.csv'
if not SNAPSHOT.exists() or '--refresh-source' in sys.argv:SNAPSHOT.write_bytes(SOURCE.read_bytes())
raw=SNAPSHOT.read_bytes()
rows=list(csv.DictReader(raw.decode('utf-8-sig').splitlines()))
KEY=['engine','profile','query_id','concurrency','run_number']
NUM=['throughput_qps','latency_p50_ms','latency_p95_ms','latency_p99_ms','cpu_throttled_seconds','max_rss_mb']
audit=[];real=[]
for r in rows:
    source=ROOT/'results/official-clean-20261101a'/r.get('source_dir','')
    p=source/'summary.csv';reason='sin CSV de origen';diff=[]
    if p.exists():
        s=next(csv.DictReader(p.open()))
        if any(r[k]!=s[k] for k in KEY):reason='escenario distinto al origen'
        else:
            for k in s:
                if r.get(k,'')==s[k]:continue
                try:
                    tol=Decimal('0.00001') if k=='timestamp_epoch' else Decimal('0.000001')
                    if abs(Decimal(r.get(k,''))-Decimal(s[k]))<=tol:continue
                except Exception:pass
                diff.append(k)
            reason='valores distintos al origen' if diff else 'verificada'
    a={**{k:r[k] for k in KEY},'source_dir':r.get('source_dir'),'clasificacion':reason,'campos_diferentes':diff}
    audit.append(a)
    if reason=='verificada':
        x=dict(r);x['_path']=source;x['_quality']=json.loads((source/'quality.json').read_text());real.append(x)
valid=[r for r in real if r['status'] in ('ok','spill') and r['_quality'].get('measurement_valid')]
counts=dict(collections.Counter(a['clasificacion'] for a in audit))
report={'sha256_csv':hashlib.sha256(raw).hexdigest(),'filas_csv':len(rows),'clasificacion':counts,'filas_reales':len(real),'ventanas_validas':sum(bool(r['_quality'].get('measurement_valid')) for r in real),'corridas_ok_spill_con_ventana':len(valid),'estados_reales':dict(collections.Counter(r['status'] for r in real)),'criterio':'Clave exacta motor/perfil/consulta/concurrencia/repetición y métricas coincidentes con summary.csv de origen. Tolerancia por formato: timestamp 10 microsegundos y restantes números 0.000001.','filas':audit}
(OUT/'auditoria_reporte.json').write_text(json.dumps(report,ensure_ascii=False,indent=2))
PROFILES=['PROFILE_A_BASELINE','PROFILE_B_MEDIUM','PROFILE_C_SEVERE'];QS=['Q1','Q2','Q2_ALT'];ENG=['mongodb','postgresql'];LABEL={'mongodb':'MongoDB','postgresql':'PostgreSQL'}
def median(rs,k):
    v=[float(r[k]) for r in rs if r.get(k) not in ('',None)]
    return statistics.median(v) if v else None
def group(q,e,p,c,source=valid):return [r for r in source if r['query_id']==q and r['engine']==e and r['profile']==p and r['concurrency']==str(c)]
def fmt(v,n=2):return 's/d' if v is None else f'{v:.{n}f}'
plt.rcParams.update({'font.family':'Arial','font.size':12,'text.color':'black','axes.labelcolor':'black','xtick.color':'black','ytick.color':'black','figure.facecolor':'white'})
styles=['-','--',':'];markers=['o','s']
for metric,name,ylabel in [('latency_p99_ms','p99_concurrencia','P99 (ms, escala logarítmica)'),('max_rss_mb','rss_concurrencia','RSS máximo (MiB)')]:
    fig,axes=plt.subplots(1,3,figsize=(10,3.6))
    for ax,q in zip(axes,QS):
        for ei,e in enumerate(ENG):
            for pi,p in enumerate(PROFILES):
                ys=[median(group(q,e,p,c),metric) for c in [1,10,50]]
                ax.plot([0,1,2],[v if v is not None else math.nan for v in ys],color='black' if ei==0 else '.48',ls=styles[pi],marker=markers[ei],label=f'{LABEL[e]} {"ABC"[pi]}',markersize=4)
        ax.set_title(q);ax.set_xticks([0,1,2],['1','10','50']);ax.set_xlabel('Clientes');ax.grid(alpha=.18)
        if metric.startswith('latency'):ax.set_yscale('log')
    axes[0].set_ylabel(ylabel);handles,labels=axes[0].get_legend_handles_labels();fig.legend(handles,labels,loc='lower center',ncol=3,fontsize=11,bbox_to_anchor=(.5,-.02));fig.tight_layout(rect=(0,.14,1,1));fig.savefig(FIG/f'{name}.png',dpi=220,bbox_inches='tight');plt.close(fig)
fig,axes=plt.subplots(1,3,figsize=(10,3.6))
for ax,q in zip(axes,QS):
    for ei,e in enumerate(ENG):
        for pi,p in enumerate(PROFILES):
            rs=[r for r in valid if (r['query_id'],r['engine'],r['profile'])==(q,e,p)]
            ax.scatter([float(r['cpu_throttled_seconds']) for r in rs],[float(r['throughput_qps']) for r in rs],marker=['o','s','^'][pi],facecolors='none' if ei==0 else '.55',edgecolors='black',s=27,label=f'{LABEL[e]} {"ABC"[pi]}')
    ax.set_title(q);ax.set_xlabel('Throttling acumulado (s)');ax.grid(alpha=.18)
axes[0].set_ylabel('Consultas completas por segundo');handles,labels=axes[0].get_legend_handles_labels();fig.legend(handles,labels,loc='lower center',ncol=3,fontsize=11,bbox_to_anchor=(.5,-.02));fig.tight_layout(rect=(0,.14,1,1));fig.savefig(FIG/'throttling_qps.png',dpi=220,bbox_inches='tight');plt.close(fig)
# Curvas temporales reales comparables, cada una con su propio inicio de ventana.
fixed={}
for q,rep in [('Q1','3'),('Q2','1')]:
    for e in ENG:
        rs=group(q,e,PROFILES[0],1);r=next(r for r in rs if r['run_number']==rep);fixed[(q,e)]=r
fig,axes=plt.subplots(2,1,figsize=(9,4),sharex=True)
for ax,q in zip(axes,['Q1','Q2']):
    for ei,e in enumerate(ENG):
        r=fixed[(q,e)];m=json.loads((r['_path']/'telemetry/metrics.json').read_text());start=r['_quality']['window']['start_epoch'];end=r['_quality']['window']['end_epoch']
        values=m['series']['container_rss_bytes'][0]['values']
        pairs=[(v['timestamp'],v['value']) if isinstance(v,dict) else v for v in values]
        pts=[(float(t)-start,float(v)/(1024**2)) for t,v in pairs if start<=float(t)<=end]
        ax.plot([a for a,b in pts],[b for a,b in pts],color='black',ls=styles[ei],label=f'{LABEL[e]} r{r["run_number"]}')
    ax.set_title(f'{q}, perfil A, un cliente');ax.set_ylabel('RSS (MiB)');ax.grid(alpha=.18);ax.legend(fontsize=9)
axes[-1].set_xlabel('Segundos desde el inicio de cada ventana');fig.tight_layout();fig.savefig(FIG/'rss_tiempo.png',dpi=220,bbox_inches='tight');plt.close(fig)
# M3: cuatro corridas con fuente y denominador declarado.
m3=[]
for (q,e),r in fixed.items():
    a=json.loads((r['_path']/'auxiliary.json').read_text());docs=a.get('documents_processed_estimate');cpu=a.get('cpu_consumed_seconds')
    m3.append([q,LABEL[e],r['run_number'],a.get('queries_completed_in_measurement'),cpu,None if not docs or cpu is None else cpu/(docs/1000),float(r['max_rss_mb']),float(r['max_rss_mb'])/500])
(OUT/'resumen_analitico.json').write_text(json.dumps({'m3':m3,'n_verificadas':len(real),'n_validas_ok_spill':len(valid)},indent=2,ensure_ascii=False))
# DOCX en Arial y negro; índice estático paginado después del primer render.
doc=Document();sec=doc.sections[0];sec.page_width=Inches(8.5);sec.page_height=Inches(11);sec.top_margin=Inches(.75);sec.bottom_margin=Inches(.75);sec.left_margin=Inches(.8);sec.right_margin=Inches(.8)
for style in doc.styles:
    if style.type==1 or style.type==2:
        try:style.font.name='Arial';style.font.color.rgb=RGBColor(0,0,0)
        except Exception:pass
for nm,size,bold in [('Normal',11,False),('Title',16,True),('Heading 1',14,True),('Heading 2',12,True),('Caption',10,False)]:
    s=doc.styles[nm];s.font.name='Arial';s.font.size=Pt(size);s.font.bold=bold;s.font.color.rgb=RGBColor(0,0,0);s.paragraph_format.line_spacing=1.5;s.paragraph_format.space_after=Pt(7)
normal=doc.styles['Normal'];normal.paragraph_format.alignment=WD_ALIGN_PARAGRAPH.JUSTIFY
for nm in ['Heading 1','Heading 2']:doc.styles[nm].paragraph_format.keep_with_next=True
footer=sec.footer.paragraphs[0];footer.alignment=WD_ALIGN_PARAGRAPH.CENTER
f=OxmlElement('w:fldSimple');f.set(qn('w:instr'),'PAGE');footer._p.append(f)
headings=[]
pending_break=False
def pagebreak():
    global pending_break
    pending_break=True
def apply_break(pa):
    global pending_break
    if pending_break:pa.paragraph_format.page_break_before=True;pending_break=False
    return pa
def p(t,style=None):return apply_break(doc.add_paragraph(t,style))
def h(t,l=1,new=False):
    if new:pagebreak()
    apply_break(doc.add_heading(t,l))
    if l==1:headings.append(t)
def table(headers,data,width=None):
    t=doc.add_table(rows=1,cols=len(headers));t.autofit=False
    widths=[1.08,.45,.3,.4,.6,.68,.68,.72,.65,.65] if len(headers)==10 else [6.9/len(headers)]*len(headers)
    widths=[w*6.9/sum(widths) for w in widths]
    for col,w in zip(t.columns,widths):col.width=Inches(w)
    for cell,label in zip(t.rows[0].cells,headers):cell.text=str(label)
    repeat=OxmlElement('w:tblHeader');t.rows[0]._tr.get_or_add_trPr().append(repeat)
    for vals in data:
        cells=t.add_row().cells
        for cell,v in zip(cells,vals):cell.text=str(v)
    for ri,row in enumerate(t.rows):
        trpr=row._tr.get_or_add_trPr();nos=OxmlElement('w:cantSplit');trpr.append(nos)
        for ci,cell in enumerate(row.cells):
            cell.width=Inches(widths[ci])
            for para in cell.paragraphs:
                para.paragraph_format.space_after=Pt(1);para.paragraph_format.space_before=Pt(1);para.paragraph_format.line_spacing=1.5
                for run in para.runs:run.font.name='Arial';run.font.size=Pt(9);run.bold=(ri==0);run.font.color.rgb=RGBColor(0,0,0)
    spacer=p('');spacer.paragraph_format.space_after=Pt(0);spacer.paragraph_format.line_spacing=1;spacer.paragraph_format.space_before=Pt(0)
    spacer.paragraph_format.line_spacing=Pt(2)
    return t
def image(name,caption):
    pa=p('');pa.alignment=WD_ALIGN_PARAGRAPH.CENTER;pa.add_run().add_picture(str(FIG/name),width=Inches(6.8));p(caption,'Caption')

doc.add_paragraph('Agregaciones JSON bajo contención de CPU y memoria','Title')
p('MongoDB y PostgreSQL JSONB sobre eventos de seguridad de red')
p('Bases de Datos en la Nube · Ingeniería en Tecnologías de la Información e Innovación Digital · Universidad Politécnica de Chiapas')
p('El laboratorio compara cómo responden dos motores a agregaciones sobre documentos anidados cuando comparten límites de CPU y memoria. Se examinan la latencia, las consultas completas por segundo y el uso de recursos. El alcance incluye Q1, que desanida y agrupa detecciones, y Q2, que recupera los mil incidentes confirmados con mayor riesgo. Los resultados corresponden al equipo y a las consultas documentados; no representan todos los usos de MongoDB ni de PostgreSQL.')
p(f'El análisis parte del CSV unificado de {len(rows)} filas. La verificación contra los artefactos identifica {len(real)} corridas del escenario correspondiente y {len(rows)-len(real)} filas que remiten a escenarios diferentes. Estas últimas conservan valor como plantillas, pero quedan excluidas de las mediciones y de las conclusiones. [2]')
doc.add_heading('Índice',1)
TOC=['Especificaciones del host y del entorno Docker','Método de medición y verificación del baseline','Resultados consolidados','Análisis gráfico','Planes de ejecución','Interpretación de memoria disco y throttling','Recomendaciones técnicas para despliegues en la nube','Fuentes y trazabilidad']
index_pages=json.loads((OUT/'index_pages.json').read_text()) if (OUT/'index_pages.json').exists() else {}
for heading in TOC:
    pa=p(heading+'\t'+str(index_pages.get(heading,'')));pa.paragraph_format.tab_stops.add_tab_stop(Inches(6.7),WD_TAB_ALIGNMENT.RIGHT,WD_TAB_LEADER.DOTS)

h(TOC[0],new=True)
p('Las pruebas se realizaron en una ASUS Vivobook Go E1504FA con AMD Ryzen 5 7520U, cuatro núcleos físicos y ocho procesadores lógicos. El diagnóstico registra Ubuntu 24.04.4 LTS, kernel 7.0.0-38-generic, memoria de aproximadamente 15 GiB y 4 GiB de swap del host. El almacenamiento corresponde a una unidad NVMe Samsung MZVL4512HBLU-00BTW. Los datos del inventario son una captura del entorno, no una medida de memoria libre durante todas las corridas. [3]')
p('Docker Engine 29.1.3 y Compose 2.40.3 aplican cuotas mediante cgroups v2. Las imágenes del proyecto fijan MongoDB 9.0.4 con WiredTiger y PostgreSQL 18.6 mediante digest. Los servicios de observabilidad son Prometheus 3.15.1, cAdvisor 0.60.6 y Node Exporter 1.12.1. Su función es observar las ventanas de cada motor sin ejecutar ambos motores bajo prueba al mismo tiempo. [3]')
table(['Perfil','CPU','RAM','Swap del contenedor'],[['A · Baseline','4','4096 MiB','0'],['B · Contención media','2','2048 MiB','0'],['C · Contención severa','1','1024 MiB','0']])
p('La prohibición de swap se comprobó mediante memory.swap.max=0. Esta equivalencia operacional sustituye la opción mem_swappiness del playbook, cuya aplicación cambia con cgroups v2. La configuración de Docker mantiene el total memoria más swap igual al límite de RAM. [1, 3, 7]')
p('PostgreSQL usa shared_buffers=256MB, hash_mem_multiplier=1 y paralelismo de consulta desactivado en los planes inspeccionados. Q2 fija work_mem=4MB y Q2_ALT lo eleva a 64MB. MongoDB usa allowDiskUse=false en Q2 y true en Q2_ALT. Son tratamientos de memoria diferentes por motor; no constituyen cantidades equivalentes de RAM. [3, 4]')

h(TOC[1],new=True)
h('Dataset consultas e índices',2)
p('Un único JSONL canónico contiene 500 000 eventos sintéticos, cada uno con dispositivo, ubicación, incidente y arreglo de detecciones. Ambos motores recibieron los mismos eventos. MongoDB los guarda como documentos en events; PostgreSQL usa events(id UUID PRIMARY KEY, data JSONB NOT NULL). La semilla y las distribuciones están en el manifiesto del generador. El archivo ocupa 349 196 514 bytes y su SHA-256 es db1f045711ad2d923921f66ab09a5129cfc46f9ddca218820acee8c8007fb9f3. [3]')
p('La adaptación conserva la estructura del ejercicio original de órdenes de compra: los ítems se convierten en detecciones y qty × price se sustituye por occurrences × impact_score. Q1 desanida detections, conserva impactos mayores que 50 y agrupa suma, promedio y conteo por país y categoría. El conteo se refiere a detecciones filtradas. Q2 selecciona incidentes confirmados desde el 1 de enero de 2026, ordena risk_score de mayor a menor y devuelve hasta mil registros. [1, 3]')
p('Se verificaron los 500 000 identificadores únicos y los índices acordados: identidad y fecha en ambos motores, sin índice por riesgo. Q1 y Q2 se contrastaron con PostgreSQL y MongoDB antes del benchmark. Q2 puede tener empates en el riesgo: se comprobó elegibilidad del corte y equivalencia semántica sin exigir que los UUID empatados aparezcan en idéntico orden. [3]')
h('Ventana y extracción de métricas',2)
p('La matriz extendida prevé 162 corridas: dos motores, tres perfiles, Q1/Q2/Q2_ALT, tres concurrencias y tres repeticiones. El orquestador alterna motores por pares de escenario con orden aleatorizado y semilla fija. Los clientes mantienen una operación activa por conexión y consumen todos los resultados. La ejecución se organiza en 60 s de calentamiento, 300 s de admisión y medición, y 60 s de enfriamiento, con drenaje acotado. [1, 3]')
p('QPS es el número de consultas exitosas completas dentro de la ventana dividido entre su duración efectiva. P50, P95 y P99 se calculan por rango más cercano sobre las latencias de esas consultas. Warmup y drenaje quedan fuera de los percentiles. El límite de consulta es 120 s: su expiración se registra como error del motor y no equivale a OOM. Cuando el error impide abrir la ventana, las métricas se conservan vacías. [3]')
p('Prometheus captura series de contenedor y host con paso de un segundo. RSS se toma de container_memory_rss y se expresa en MiB, aunque la columna contractual se llama max_rss_mb. Throttling representa el incremento del contador de tiempo de CPU restringido; no es CPU consumida ni duración de pared. Se conserva CPU consumida aparte para M3. [3]')
p('Spill se establece con logs correlacionados a la consulta: usedDisk en MongoDB y archivos temporales en PostgreSQL. Su ausencia solo se acepta como negativa con cobertura suficiente. OOM exige evidencia Docker/cgroup. EXPLAIN se ejecutó como diagnóstico separado. La caché del host no se limpió: el protocolo es una adaptación y no reproduce exactamente el flush del playbook. [1, 3]')
h('Procedencia y alcance del corte',2)
p(f'El CSV de entrada contiene {len(rows)} combinaciones únicas y una columna source_dir. La clave motor, perfil, consulta, concurrencia y repetición coincide con la corrida de origen en {len(real)} filas ({len(real)/len(rows)*100:.1f} %). Las otras {len(rows)-len(real)} reutilizan resultados de otra combinación. La auditoría del proyecto identifica este material como fixture offline. El reporte conserva la cobertura incompleta y no convierte esas filas en mediciones. [2]')
p(f'De las {len(real)} filas verificadas, {len(valid)} terminaron ok o spill con ventana válida. Se conservaron también ocho resultados error y uno oom. Seis fallos no llegaron a medir y tres tuvieron ventana pero terminaron error u oom. Las medianas y gráficas comparativas usan las {len(valid)} corridas ok/spill; los fallos se presentan por separado para no ocultar la tasa de resultados adversos. La selección puede favorecer las ejecuciones que sí completaron el protocolo y debe tenerse presente al comparar.')

h(TOC[2],new=True)
p('Las tablas agrupan por motor, perfil, consulta y concurrencia. n indica cuántas repeticiones ok/spill con ventana válida respaldan la mediana, de tres previstas. P95 y P99 son medianas de percentiles por corrida, no percentiles de una distribución combinada. s/d significa que no existe una medición admisible para ese resumen. Las columnas de fallos cuentan todos los resultados reales del grupo.')
for qi,q in enumerate(QS):
    if qi:pagebreak()
    h({'Q1':'Q1 Desanidar filtrar y agrupar','Q2':'Q2 Ordenamiento con tratamiento base','Q2_ALT':'Q2 ALT Ordenamiento con tratamiento alternativo'}[q],2)
    data=[]
    for p0 in PROFILES:
        for c in [1,10,50]:
            for e in ENG:
                rs=group(q,e,p0,c);allr=group(q,e,p0,c,real);err=sum(r['status']=='error' for r in allr);oom=sum(r['status']=='oom' for r in allr)
                data.append([LABEL[e],p0[8],c,f'{len(rs)}/3',fmt(median(rs,'throughput_qps'),3),fmt(median(rs,'latency_p95_ms'),0),fmt(median(rs,'latency_p99_ms'),0),fmt(median(rs,'cpu_throttled_seconds'),1),fmt(median(rs,'max_rss_mb'),1),f'{err}/{oom}'])
    table(['Motor','Perfil','c','n','QPS','P95\nms','P99\nms','Throt.\ns','RSS\nMiB','Err./\nOOM'],data)
    p({'Q1':'Q1 conserva spill en las once corridas PostgreSQL que finalizaron con medición admisible. MongoDB presenta once corridas ok; en perfiles B y C, las combinaciones de cincuenta clientes muestran errores o un OOM, según el motor. En perfil A, con una sola repetición por concurrencia, MongoDB logra 0.503 QPS con un cliente y 1.167 con cincuenta, mientras P99 aumenta de 2.17 a 46.41 s. PostgreSQL pasa de 0.407 a 0.857 QPS y de 2.86 a 64.20 s.','Q2':'En el perfil A con un cliente, Q2 alcanza 2.840 QPS en PostgreSQL y 0.193 en MongoDB, ambos con una repetición. Con diez clientes las medianas disponibles son 5.420 y 0.867 QPS. El perfil C a cincuenta clientes produjo dos timeouts de MongoDB sin ventana y dos corridas PostgreSQL ok; la comparación incluye tanto rendimiento como posibilidad de completar la carga.','Q2_ALT':'Q2_ALT conserva la misma consulta y cambia el tratamiento de memoria. En A con diez clientes PostgreSQL registra una mediana de 5.468 QPS y MongoDB de 0.850, con dos repeticiones cada uno. Las celdas faltantes impiden afirmar una mejora general frente a Q2. El perfil C con un cliente sí tiene tres repeticiones por motor en esta variante.'}[q])

h(TOC[3],new=True)
image('p99_concurrencia.png','Figura 1. Mediana de P99 por consulta, motor, perfil y concurrencia. Solo corridas verificadas ok/spill con ventana. Los huecos representan falta de datos admisibles; el eje vertical es logarítmico. Fuente: CSV unificado contrastado con sus corridas.')
p('En Q1 con diez clientes, el perfil C registra P99 medianos de 59.54 s en MongoDB y 64.08 s en PostgreSQL. Las ausencias a cincuenta clientes corresponden a resultados adversos o celdas sin medición; no representan latencias iguales a cero.')
image('throttling_qps.png','Figura 2. Throttling acumulado frente a QPS. Cada punto corresponde a una corrida admisible; forma indica perfil y relleno indica motor. No se mezclan plantillas ni errores con las ejecuciones ok/spill.')
p('El gráfico no aísla el efecto de la cuota: también cambian consulta y concurrencia. Throttling es un contador acumulado, no tiempo adicional de ejecución. Para dimensionar un servicio hay que leer QPS junto con percentiles y errores.')
pagebreak()
image('rss_concurrencia.png','Figura 3. Mediana de RSS máximo por concurrencia. Se conserva la misma selección de corridas que en la Figura 1. RSS describe residencia en memoria y no el tamaño total del dataset ni el consumo acumulado.')
p('MongoDB mantiene una ocupación residente mayor que PostgreSQL en varias celdas de baja concurrencia. Esto no prueba por sí solo menor eficiencia: los motores distribuyen de forma distinta cachés, buffers y trabajo temporal. La interpretación se apoya en el plan, los archivos temporales y los resultados de estabilidad, no solamente en el pico de RSS.')
image('rss_tiempo.png','Figura 4. RSS durante ventanas reales de 300 s en perfil A y un cliente. Q1 usa repetición 3 y Q2 repetición 1 en ambos motores. El tiempo se alinea al inicio de cada corrida; las curvas no corresponden a motores ejecutados simultáneamente.')
p('Las curvas permiten distinguir un nivel residente sostenido de un pico aislado. Proceden de telemetry/metrics.json, no de una interpolación del máximo del CSV. El periodo observado es corto y no constituye una prueba de crecimiento sostenido de memoria ni de fuga.')

h(TOC[4],new=True)
p('Los planes de fase 04 corresponden al perfil A y se tomaron fuera de las ventanas de benchmark. Sirven para explicar recorridos, ordenamientos y agrupaciones. Sus tiempos no se incorporan a los percentiles de las tablas. [4]')
h('Q1 Agrupación sobre el arreglo anidado',2)
p('MongoDB recorre 500 000 documentos mediante COLLSCAN. El unwind produce 1 499 811 detecciones, el filtro conserva 741 897 y el grupo devuelve treinta combinaciones. La captura registra usedDisk=false y spills=0 para la agrupación. Esta evidencia diagnóstica concuerda con los logs negativos de spill de las corridas verificadas que terminaron ok.')
p('PostgreSQL recorre 500 000 filas y aplica jsonb_array_elements en un Nested Loop. Ordena 741 897 elementos antes de agrupar en treinta resultados. El Sort usa external merge y 129 824 kB en disco; el plan registra 32 492 bloques temporales escritos. La estimación de 16 510 032 filas previas al grupo supera las 741 897 observadas, una diferencia relevante para interpretar la estrategia escogida. No se atribuye toda la latencia al error de estimación sin una prueba adicional.')
h('Q2 Selección y Top K',2)
p('MongoDB usa timestamp_idx mediante IXSCAN y FETCH: examina 249 982 claves y documentos, conserva 150 190 elegibles y devuelve mil. La ordenación no presenta spill en la captura ni con allowDiskUse=false ni con true. El límite Top-K evita que el número de filas de salida represente el trabajo total de lectura.')
p('PostgreSQL usa Seq Scan, filtra 150 190 registros elegibles y conserva mil mediante top-N heapsort. El ordenamiento ocupa 271 kB en memoria, tanto con work_mem=4MB como con 64MB, y no registra bloques temporales. La ausencia de mejora automática al ampliar work_mem es coherente con un Top-K que ya cabe en el presupuesto base. No se usa el plan para afirmar que todas las corridas tuvieron exactamente ese consumo.')
table(['Consulta','MongoDB','PostgreSQL'],[['Q1','COLLSCAN → unwind → filtro → group','Seq Scan → Function Scan → Sort en disco → Aggregate'],['Q2 / Q2_ALT','IXSCAN → FETCH → SORT → límite 1000','Seq Scan → top-N heapsort → Limit']])

h(TOC[5],new=True)
h('M1 Cambios en P95 y P99 al limitar recursos',2)
p('La evidencia de Q1 indica que la contención se vuelve más visible con varios clientes. Con diez clientes, el perfil A registra P99 de 10.78 s en MongoDB y 12.76 s en PostgreSQL, con una repetición por motor. En perfil C las medianas ascienden a 59.54 y 64.08 s, con dos repeticiones. P95 sigue la misma dirección en los datos disponibles. Al mismo tiempo, QPS cae de 1.163 a 0.205 en MongoDB y de 0.920 a 0.165 en PostgreSQL. Estas cifras describen las celdas observadas. Como el diseño cambia CPU y RAM de forma conjunta, no permite separar el efecto marginal de cada recurso.')
p('Q2 presenta una diferencia de motor marcada: en perfil C con diez clientes, P99 es 44.50 s en MongoDB y 10.69 s en PostgreSQL. Cada cifra procede de una repetición. El efecto no se explica solo por RAM: los planes muestran distinta cantidad y forma de trabajo. Las curvas se interrumpen donde hubo timeout, OOM o ausencia de escenarios, para evitar representar esas situaciones como mejor latencia.')
h('M2 Buffers antes de spill u OOM',2)
p('Para Q1, PostgreSQL vuelca a disco en once corridas exitosas verificadas y el plan exhibe external merge. MongoDB completa once corridas Q1 sin spill comprobado. En esta consulta y con estos parámetros, MongoDB evita el trabajo temporal que sí se observa en PostgreSQL. Sin embargo, la concurrencia extrema también produce timeouts de MongoDB y un resultado OOM de PostgreSQL; por ello no corresponde declarar superioridad general de un motor en el uso de buffers.')
p('En Q2 el top-N de PostgreSQL cabe en 271 kB en el diagnóstico, y MongoDB tampoco registra spill en el plan. Cambiar allowDiskUse o work_mem no obliga a utilizar disco: habilitar una alternativa no demuestra que el motor la necesite. La documentación de MongoDB distingue el permiso de spill de la señal usedDisk; PostgreSQL define work_mem por operación, de modo que varias operaciones y conexiones pueden multiplicar su demanda. [5, 6]')
p('Los ocho resultados error y el OOM se conservan como observaciones. En los timeouts sin ventana, no existen percentiles de medición. Dos corridas MongoDB sí abrieron ventana y completaron algunas consultas antes de terminar error; sus percentiles corresponden solo a los éxitos y están condicionados por el corte de 120 s. Se excluyen del resumen de ejecuciones completas. El OOM del host que interrumpió un intento previo se documenta como infraestructura y se mantiene aparte del reintento terminal del motor. [3]')
h('M3 CPU y memoria por mil documentos',2)
p('La CPU consumida no aparece en el CSV contractual como tal: cpu_throttled_seconds mide restricción. Para responder M3 se usaron los sidecars auxiliary.json de cuatro corridas verificadas del perfil A con un cliente. La estimación divide CPU consumida entre documentos examinados estimados por consulta, multiplicados por las consultas completas. El denominador procede de los planes separados: 500 000 documentos por Q1 en ambos motores, 249 982 por Q2 en MongoDB y 500 000 por Q2 en PostgreSQL. [3, 4]')
p('CPU por mil = segundos de CPU / (documentos estimados procesados / 1000). Esta tasa es aproximada: las operaciones en vuelo también consumen CPU y los planes se capturaron en otra ejecución. Para memoria se informa RSS máximo y una normalización por mil eventos del dataset: RSS / 500. Esta última describe ocupación relativa del conjunto de datos y no memoria incremental por documento examinado.')
table(['Consulta','Motor','Rep.','Éxitos','CPU\ns','CPU s/\n1000 docs','RSS\nMiB','MiB/1000\neventos'],[[q,e,rep,n,fmt(cpu,2),fmt(rate,5),fmt(rss,2),fmt(norm,3)] for q,e,rep,n,cpu,rate,rss,norm in m3])
p('Estas cuatro observaciones permiten comparar el trabajo bajo el mismo perfil y concurrencia, pero no estimar por sí solas la huella de todas las combinaciones. La tasa usa documentos de entrada examinados, no las mil filas de salida de Q2 ni las detecciones de Q1 como si fueran documentos originales. Una comparación completa de M3 requiere terminar la matriz y comprobar la continuidad de los contadores en cada corrida.')
p('En estas corridas, Q1 estima 0.00445 s de CPU por mil documentos en MongoDB y 0.00525 en PostgreSQL. Para Q2 las tasas son 0.02373 y 0.00086 s, respectivamente, con distinto número de documentos examinados por consulta. MongoDB registra mayor RSS en ambas cargas. Son observaciones del perfil A y un cliente, suficientes para describir esos casos pero no para establecer una ventaja universal de CPU o memoria.')

h(TOC[6],new=True)
p('Para agregaciones Q1 de este dominio, las corridas observadas favorecen a MongoDB en QPS y evitan spill en los casos que completaron la medición. Antes de elegir una instancia pequeña, conviene limitar las consultas simultáneas y comprobar P99: aumentar clientes elevó la latencia mucho más que el throughput. El perfil C con cincuenta clientes no representa una configuración estable para esta carga.')
p('PostgreSQL muestra mayor QPS en Q2 entre las celdas comparables verificadas. Para un servicio centrado en búsquedas Top-K sobre incidentes, puede ser una opción adecuada si la estructura JSONB y sus índices responden a la carga real. El diagnóstico Top-N que cabe en memoria no justifica aumentar work_mem globalmente: el ajuste debe probarse por sesión y con el número de conexiones esperado. [5]')
p('La indexación por estado, fecha y riesgo es una propuesta para un ensayo posterior. Debe evaluarse con planes y mediciones nuevas, porque un índice adicional cambia el experimento y puede aumentar almacenamiento y costo de escritura. Del mismo modo, columnas extraídas o vistas materializadas para atributos usados en agrupación son alternativas de diseño, no optimizaciones ya comprobadas en estas corridas.')
p('En despliegues con contenedores se recomienda vigilar throttling, errores de consulta, RSS y presión del host en conjunto. Mantener límites de memoria y margen para el generador de carga permite distinguir un fallo del motor de una interrupción del laboratorio. Deben conservarse tanto intentos fallidos como recuperaciones para que una tasa de éxito alta no esconda problemas de capacidad. [7]')
p('La práctica muestra que una consulta rápida con un cliente puede cambiar de comportamiento al compartir recursos con diez o cincuenta. La elección de motor depende de la operación que concentra el trabajo: el agrupamiento de detecciones y la selección Top-K no producen el mismo patrón de latencia ni de uso de memoria. Continuar las repeticiones pendientes permitirá comprobar la estabilidad de estos patrones y precisar los límites de concurrencia antes de trasladarlos a un servicio en la nube.')

h(TOC[7],new=True)
refs=[
'[1] Universidad Politécnica de Chiapas. Medición de métricas en bases documentales. Bases de Datos en la Nube, IT2iD, 2026. Guía local docs/fuentes/playbook.pdf, pp. 1–4.',
f'[2] CSV de análisis results/results_unificado_162.csv. {len(rows)} filas. SHA-256: {report["sha256_csv"]}. Contraste reproducible en output/reporte-metricas/auditoria_reporte.json. Las métricas se respaldan en results/official-clean-20261101a/<source_dir>/.',
'[3] Documentación operativa del laboratorio: context/alcance.md; context/adaptacion_consultas.md; contracts/medicion.md; docs/estado-actual.md; docs/auditoria-piloto.md; docs/entorno.json; config/images.lock.json; manifiesto data/generated/events.jsonl.manifest.json. Los estados históricos se interpretan según su fecha y fase.',
'[4] Planes de fase 04: results/plans/phase04-profile-a/mongodb-q1-fixed-current.json, mongodb-q2-base-current.json, mongodb-q2-alt-current.json, postgresql-q1-fixed-retry.json, postgresql-q2-base.json y postgresql-q2-alt.json. Se consultaron artefactos guardados, sin volver a ejecutar consultas.',
'[5] PostgreSQL Global Development Group. PostgreSQL 18 Documentation, Resource Consumption. https://www.postgresql.org/docs/18/runtime-config-resource.html. Consulta: 1 de noviembre de 2026.',
'[6] MongoDB. Aggregation Pipeline Limits. https://www.mongodb.com/docs/manual/core/aggregation-pipeline-limits/. Consulta: 1 de noviembre de 2026.',
'[7] Docker. Resource constraints. https://docs.docker.com/engine/containers/resource_constraints/. Consulta: 1 de noviembre de 2026.'
]
for ref in refs:p(ref)
p('Las Figuras 1–3 se calcularon únicamente a partir de las filas verificadas del CSV de entrada y sus archivos de calidad. La Figura 4 utiliza series temporales guardadas de las corridas indicadas. Las unidades y la exclusión de plantillas se mantienen en las figuras y en las tablas. El script scripts/build_metrics_report.py reproduce el análisis y el documento sin consultar datos de otros proyectos.')
doc.core_properties.title='Agregaciones JSON bajo contención de CPU y memoria';doc.core_properties.subject='Reporte de métricas con procedencia de corridas';doc.core_properties.author='';doc.core_properties.keywords='MongoDB PostgreSQL JSONB benchmark'
# Evitar colores de tema en vínculos y números.
for para in doc.paragraphs:
    for run in para.runs:
        run.font.name='Arial';run.font.color.rgb=RGBColor(0,0,0)
for root in [doc._element,doc.styles.element]:
    for border in list(root.iter(qn('w:pBdr'))):border.getparent().remove(border)
doc.save(OUT/'reporte_metricas.docx')
print(json.dumps({k:v for k,v in report.items() if k!='filas'},ensure_ascii=False,indent=2));print('M3',m3)
