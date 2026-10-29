"""Measure ICMP timing without writing the target address into JSON output."""
import argparse,json,math,platform,re,statistics,subprocess,sys
p=argparse.ArgumentParser();p.add_argument('--host',required=True);p.add_argument('--count',type=int,default=20);a=p.parse_args()
if not 1<=a.count<=100:p.error('count must be 1–100')
flag='-n' if platform.system()=='Windows' else '-c'
r=subprocess.run(['ping',flag,str(a.count),a.host],capture_output=True,text=True,timeout=a.count*2+10)
values=[];bounds=0
for line in r.stdout.splitlines():
 m=re.search(r'(?i)time\s*([=<])\s*(\d+(?:\.\d+)?)\s*ms',line)
 if m:
  values.append(float(m.group(2)));bounds+=m.group(1)=='<'
if not values:raise SystemExit('No parseable ICMP replies; inspect local ping output without publishing identifiers')
ordered=sorted(values);result={'sent':a.count,'received':len(values),'loss_count':a.count-len(values),'bounded_less_than_readings':bounds,
 'median_ms':statistics.median(values),'p95_ms':ordered[math.ceil(.95*len(values))-1],'min_ms':min(values),'max_ms':max(values),'samples_ms':values,
 'note':'A less-than reading is represented by its displayed upper bound; platform/localization-dependent ping parsing.'}
print(json.dumps(result,indent=2))
