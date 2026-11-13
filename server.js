const http=require("http"),fs=require("fs"),path=require("path"),crypto=require("crypto"),{WebSocketServer}=require("ws");
const DIR=process.env.DATA_DIR||path.join(__dirname,"data"),BL=path.join(DIR,"blobs"),DBF=path.join(DIR,"db.json");
fs.mkdirSync(BL,{recursive:true});
let db=new Map();try{db=new Map(Object.entries(JSON.parse(fs.readFileSync(DBF,"utf8"))))}catch(_){}
let dirty=false;
setInterval(()=>{if(!dirty)return;dirty=false;fs.writeFile(DBF+".tmp",JSON.stringify(Object.fromEntries(db)),e=>{if(!e)fs.rename(DBF+".tmp",DBF,()=>{})})},2000);
const okPath=p=>typeof p==="string"&&p.length<200&&/^[\w-]+(\/[\w.@+-]+)*$/.test(p);
const parent=p=>p.slice(0,p.lastIndexOf("/")),socks=new Set(),rooms=new Map();
const kids=(c)=>[...db].filter(([k])=>k.startsWith(c+"/")&&!k.slice(c.length+1).includes("/")).map(([k,v])=>({id:k.slice(c.length+1),data:v}));
const send=(ws,o)=>{if(ws.readyState===1)ws.send(JSON.stringify(o))};
function fan(path,type,data){const c=parent(path),id=path.slice(c.length+1);
  for(const ws of socks)for(const[sid,s]of ws.subs)if(s.coll===c)send(ws,{op:"chg",sid,type,id,data})}
function peers(name){const r=rooms.get(name);if(!r)return;const p=[...r].map(w=>w.id);for(const w of r)send(w,{op:"peers",room:name,peers:p})}
function leave(ws,name){const r=rooms.get(name);if(!r)return;r.delete(ws);ws.rooms.delete(name);if(!r.size)rooms.delete(name);else peers(name)}
const server=http.createServer((req,res)=>{
  const u=req.url.split("?")[0];
  if(u==="/healthz"){res.end("ok");return}
  if(req.method==="POST"&&u==="/_upload"){
    const ch=[];let n=0;
    req.on("data",d=>{n+=d.length;if(n>21*1024*1024){res.statusCode=413;res.end();req.destroy()}else ch.push(d)});
    req.on("end",()=>{if(n>21*1024*1024)return;const id=crypto.randomBytes(16).toString("hex"),t=String(req.headers["content-type"]||"");
      fs.writeFileSync(path.join(BL,id),Buffer.concat(ch));fs.writeFileSync(path.join(BL,id+".type"),/^[\w.+-]+\/[\w.+-]+$/.test(t)?t:"application/octet-stream");
      res.setHeader("Content-Type","application/json");res.end(JSON.stringify({id}))});return}
  const m=u.match(/^\/_blob\/([0-9a-f]{32})$/);
  if(m){try{const b=fs.readFileSync(path.join(BL,m[1])),t=fs.readFileSync(path.join(BL,m[1]+".type"),"utf8");
    res.setHeader("Content-Type",t);res.setHeader("X-Content-Type-Options","nosniff");res.setHeader("Cache-Control","public,max-age=31536000,immutable");res.end(b)}catch(_){res.statusCode=404;res.end("not found")}return}
  if(u==="/"||u==="/index.html"){res.setHeader("Content-Type","text/html; charset=utf-8");res.end(fs.readFileSync(path.join(__dirname,"public","index.html")));return}
  res.statusCode=404;res.end("not found")});
const wss=new WebSocketServer({server,path:"/ws",maxPayload:1024*1024});
wss.on("connection",ws=>{
  ws.id=crypto.randomBytes(6).toString("hex");ws.subs=new Map();ws.rooms=new Set();ws.alive=true;ws.hits=[];socks.add(ws);
  ws.on("pong",()=>ws.alive=true);
  ws.on("message",raw=>{let m;try{m=JSON.parse(raw)}catch(_){return}
    const ack=(ok,data,err)=>m.rid&&send(ws,{op:"ack",rid:m.rid,ok,data,err});
    const now=Date.now();ws.hits=ws.hits.filter(t=>now-t<10000);
    if(["set","upd","del","emit"].includes(m.op)){if(ws.hits.length>=40){ack(false,null,"slow down");return}ws.hits.push(now)}
    switch(m.op){
      case"sub":if(!okPath(m.coll))return;{let d=kids(m.coll);const ord=/^\w+$/.test(m.ord||"")?m.ord:null,dir=m.dir==="desc"?-1:1;
        if(ord)d.sort((a,b)=>((a.data[ord]>b.data[ord])-(a.data[ord]<b.data[ord]))*dir);if(m.limit>0)d=d.slice(0,Math.min(500,m.limit));
        ws.subs.set(m.sid,{coll:m.coll});send(ws,{op:"init",sid:m.sid,docs:d})}break;
      case"unsub":ws.subs.delete(m.sid);break;
      case"get":if(!okPath(m.path))return ack(false,null,"bad path");ack(true,db.has(m.path)?db.get(m.path):null);break;
      case"set":case"upd":{if(!okPath(m.path)||!m.data||typeof m.data!=="object"||Array.isArray(m.data)||JSON.stringify(m.data).length>300000)return ack(false,null,"bad write");
        const had=db.has(m.path),v=m.op==="upd"&&had?Object.assign({},db.get(m.path),m.data):m.data;db.set(m.path,v);dirty=true;fan(m.path,had?"modified":"added",v);ack(true)}break;
      case"del":if(!okPath(m.path))return ack(false,null,"bad path");if(db.has(m.path)){const o=db.get(m.path);db.delete(m.path);dirty=true;fan(m.path,"removed",o)}ack(true);break;
      case"join":if(typeof m.room!=="string"||m.room.length>80)return;leave(ws,m.room);if(!rooms.has(m.room))rooms.set(m.room,new Set());rooms.get(m.room).add(ws);ws.rooms.add(m.room);peers(m.room);break;
      case"leave":leave(ws,m.room);break;
      case"emit":if(ws.rooms.has(m.room)&&JSON.stringify(m.data||{}).length<2000)for(const w of rooms.get(m.room))if(w!==ws)send(w,{op:"room",room:m.room,topic:String(m.topic).slice(0,20),data:m.data,from:ws.id});break}});
  ws.on("close",()=>{socks.delete(ws);for(const r of [...ws.rooms])leave(ws,r)})});
setInterval(()=>{for(const ws of wss.clients){if(!ws.alive){ws.terminate();continue}ws.alive=false;ws.ping()}},30000);
server.listen(process.env.PORT||3000,()=>console.log("MORSE//CHAT on :"+(process.env.PORT||3000)));
