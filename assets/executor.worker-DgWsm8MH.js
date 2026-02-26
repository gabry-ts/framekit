var Ge=Object.defineProperty,qe=(t,n)=>{for(var r in n)Ge(t,r,{get:n[r],enumerable:!0})},xe={};qe(xe,{analyzeMetafile:()=>ft,analyzeMetafileSync:()=>gt,build:()=>at,buildSync:()=>dt,context:()=>ct,default:()=>yt,formatMessages:()=>ut,formatMessagesSync:()=>mt,initialize:()=>Le,stop:()=>pt,transform:()=>Ne,transformSync:()=>ht,version:()=>ot});function ke(t){let n=a=>{if(a===null)r.write8(0);else if(typeof a=="boolean")r.write8(1),r.write8(+a);else if(typeof a=="number")r.write8(2),r.write32(a|0);else if(typeof a=="string")r.write8(3),r.write(ee(a));else if(a instanceof Uint8Array)r.write8(4),r.write(a);else if(a instanceof Array){r.write8(5),r.write32(a.length);for(let m of a)n(m)}else{let m=Object.keys(a);r.write8(6),r.write32(m.length);for(let g of m)r.write(ee(g)),n(a[g])}},r=new Oe;return r.write32(0),r.write32(t.id<<1|+!t.isRequest),n(t.value),ve(r.buf,r.len-4,0),r.buf.subarray(0,r.len)}function Je(t){let n=()=>{switch(r.read8()){case 0:return null;case 1:return!!r.read8();case 2:return r.read32();case 3:return oe(r.read());case 4:return r.read();case 5:{let c=r.read32(),v=[];for(let u=0;u<c;u++)v.push(n());return v}case 6:{let c=r.read32(),v={};for(let u=0;u<c;u++)v[oe(r.read())]=n();return v}default:throw new Error("Invalid packet")}},r=new Oe(t),a=r.read32(),m=(a&1)===0;a>>>=1;let g=n();if(r.ptr!==t.length)throw new Error("Invalid packet");return{id:a,isRequest:m,value:g}}var Oe=class{constructor(t=new Uint8Array(1024)){this.buf=t,this.len=0,this.ptr=0}_write(t){if(this.len+t>this.buf.length){let n=new Uint8Array((this.len+t)*2);n.set(this.buf),this.buf=n}return this.len+=t,this.len-t}write8(t){let n=this._write(1);this.buf[n]=t}write32(t){let n=this._write(4);ve(this.buf,t,n)}write(t){let n=this._write(4+t.length);ve(this.buf,t.length,n),this.buf.set(t,n+4)}_read(t){if(this.ptr+t>this.buf.length)throw new Error("Invalid packet");return this.ptr+=t,this.ptr-t}read8(){return this.buf[this._read(1)]}read32(){return Ce(this.buf,this._read(4))}read(){let t=this.read32(),n=new Uint8Array(t),r=this._read(n.length);return n.set(this.buf.subarray(r,r+t)),n}},ee,oe,be;if(typeof TextEncoder<"u"&&typeof TextDecoder<"u"){let t=new TextEncoder,n=new TextDecoder;ee=r=>t.encode(r),oe=r=>n.decode(r),be='new TextEncoder().encode("")'}else if(typeof Buffer<"u")ee=t=>Buffer.from(t),oe=t=>{let{buffer:n,byteOffset:r,byteLength:a}=t;return Buffer.from(n,r,a).toString()},be='Buffer.from("")';else throw new Error("No UTF-8 codec found");if(!(ee("")instanceof Uint8Array))throw new Error(`Invariant violation: "${be} instanceof Uint8Array" is incorrectly false

This indicates that your JavaScript environment is broken. You cannot use
esbuild in this environment because esbuild relies on this invariant. This
is not a problem with esbuild. You need to fix your environment instead.
`);function Ce(t,n){return t[n++]|t[n++]<<8|t[n++]<<16|t[n++]<<24}function ve(t,n,r){t[r++]=n,t[r++]=n>>8,t[r++]=n>>16,t[r++]=n>>24}var J=JSON.stringify,Ee="warning",_e="silent";function Se(t){if(H(t,"target"),t.indexOf(",")>=0)throw new Error(`Invalid target: ${t}`);return t}var me=()=>null,z=t=>typeof t=="boolean"?null:"a boolean",T=t=>typeof t=="string"?null:"a string",ge=t=>t instanceof RegExp?null:"a RegExp object",ie=t=>typeof t=="number"&&t===(t|0)?null:"an integer",Ae=t=>typeof t=="function"?null:"a function",G=t=>Array.isArray(t)?null:"an array",Z=t=>typeof t=="object"&&t!==null&&!Array.isArray(t)?null:"an object",Ye=t=>typeof t=="object"&&t!==null?null:"an array or an object",He=t=>t instanceof WebAssembly.Module?null:"a WebAssembly.Module",Te=t=>typeof t=="object"&&!Array.isArray(t)?null:"an object or null",De=t=>typeof t=="string"||typeof t=="boolean"?null:"a string or a boolean",Qe=t=>typeof t=="string"||typeof t=="object"&&t!==null&&!Array.isArray(t)?null:"a string or an object",Xe=t=>typeof t=="string"||Array.isArray(t)?null:"a string or an array",Re=t=>typeof t=="string"||t instanceof Uint8Array?null:"a string or a Uint8Array",Ke=t=>typeof t=="string"||t instanceof URL?null:"a string or a URL";function i(t,n,r,a){let m=t[r];if(n[r+""]=!0,m===void 0)return;let g=a(m);if(g!==null)throw new Error(`${J(r)} must be ${g}`);return m}function Y(t,n,r){for(let a in t)if(!(a in n))throw new Error(`Invalid option ${r}: ${J(a)}`)}function Ze(t){let n=Object.create(null),r=i(t,n,"wasmURL",Ke),a=i(t,n,"wasmModule",He),m=i(t,n,"worker",z);return Y(t,n,"in initialize() call"),{wasmURL:r,wasmModule:a,worker:m}}function Ue(t){let n;if(t!==void 0){n=Object.create(null);for(let r in t){let a=t[r];if(typeof a=="string"||a===!1)n[r]=a;else throw new Error(`Expected ${J(r)} in mangle cache to map to either a string or false`)}}return n}function ye(t,n,r,a,m){let g=i(n,r,"color",z),c=i(n,r,"logLevel",T),v=i(n,r,"logLimit",ie);g!==void 0?t.push(`--color=${g}`):a&&t.push("--color=true"),t.push(`--log-level=${c||m}`),t.push(`--log-limit=${v||0}`)}function H(t,n,r){if(typeof t!="string")throw new Error(`Expected value for ${n}${r!==void 0?" "+J(r):""} to be a string, got ${typeof t} instead`);return t}function Ie(t,n,r){let a=i(n,r,"legalComments",T),m=i(n,r,"sourceRoot",T),g=i(n,r,"sourcesContent",z),c=i(n,r,"target",Xe),v=i(n,r,"format",T),u=i(n,r,"globalName",T),I=i(n,r,"mangleProps",ge),U=i(n,r,"reserveProps",ge),$=i(n,r,"mangleQuoted",z),L=i(n,r,"minify",z),D=i(n,r,"minifySyntax",z),M=i(n,r,"minifyWhitespace",z),j=i(n,r,"minifyIdentifiers",z),C=i(n,r,"lineLimit",ie),k=i(n,r,"drop",G),V=i(n,r,"dropLabels",G),E=i(n,r,"charset",T),h=i(n,r,"treeShaking",z),o=i(n,r,"ignoreAnnotations",z),s=i(n,r,"jsx",T),f=i(n,r,"jsxFactory",T),w=i(n,r,"jsxFragment",T),_=i(n,r,"jsxImportSource",T),x=i(n,r,"jsxDev",z),d=i(n,r,"jsxSideEffects",z),e=i(n,r,"define",Z),l=i(n,r,"logOverride",Z),p=i(n,r,"supported",Z),y=i(n,r,"pure",G),S=i(n,r,"keepNames",z),A=i(n,r,"platform",T),R=i(n,r,"tsconfigRaw",Qe);if(a&&t.push(`--legal-comments=${a}`),m!==void 0&&t.push(`--source-root=${m}`),g!==void 0&&t.push(`--sources-content=${g}`),c&&(Array.isArray(c)?t.push(`--target=${Array.from(c).map(Se).join(",")}`):t.push(`--target=${Se(c)}`)),v&&t.push(`--format=${v}`),u&&t.push(`--global-name=${u}`),A&&t.push(`--platform=${A}`),R&&t.push(`--tsconfig-raw=${typeof R=="string"?R:JSON.stringify(R)}`),L&&t.push("--minify"),D&&t.push("--minify-syntax"),M&&t.push("--minify-whitespace"),j&&t.push("--minify-identifiers"),C&&t.push(`--line-limit=${C}`),E&&t.push(`--charset=${E}`),h!==void 0&&t.push(`--tree-shaking=${h}`),o&&t.push("--ignore-annotations"),k)for(let O of k)t.push(`--drop:${H(O,"drop")}`);if(V&&t.push(`--drop-labels=${Array.from(V).map(O=>H(O,"dropLabels")).join(",")}`),I&&t.push(`--mangle-props=${I.source}`),U&&t.push(`--reserve-props=${U.source}`),$!==void 0&&t.push(`--mangle-quoted=${$}`),s&&t.push(`--jsx=${s}`),f&&t.push(`--jsx-factory=${f}`),w&&t.push(`--jsx-fragment=${w}`),_&&t.push(`--jsx-import-source=${_}`),x&&t.push("--jsx-dev"),d&&t.push("--jsx-side-effects"),e)for(let O in e){if(O.indexOf("=")>=0)throw new Error(`Invalid define: ${O}`);t.push(`--define:${O}=${H(e[O],"define",O)}`)}if(l)for(let O in l){if(O.indexOf("=")>=0)throw new Error(`Invalid log override: ${O}`);t.push(`--log-override:${O}=${H(l[O],"log override",O)}`)}if(p)for(let O in p){if(O.indexOf("=")>=0)throw new Error(`Invalid supported: ${O}`);const P=p[O];if(typeof P!="boolean")throw new Error(`Expected value for supported ${J(O)} to be a boolean, got ${typeof P} instead`);t.push(`--supported:${O}=${P}`)}if(y)for(let O of y)t.push(`--pure:${H(O,"pure")}`);S&&t.push("--keep-names")}function et(t,n,r,a,m){var g;let c=[],v=[],u=Object.create(null),I=null,U=null;ye(c,n,u,r,a),Ie(c,n,u);let $=i(n,u,"sourcemap",De),L=i(n,u,"bundle",z),D=i(n,u,"splitting",z),M=i(n,u,"preserveSymlinks",z),j=i(n,u,"metafile",z),C=i(n,u,"outfile",T),k=i(n,u,"outdir",T),V=i(n,u,"outbase",T),E=i(n,u,"tsconfig",T),h=i(n,u,"resolveExtensions",G),o=i(n,u,"nodePaths",G),s=i(n,u,"mainFields",G),f=i(n,u,"conditions",G),w=i(n,u,"external",G),_=i(n,u,"packages",T),x=i(n,u,"alias",Z),d=i(n,u,"loader",Z),e=i(n,u,"outExtension",Z),l=i(n,u,"publicPath",T),p=i(n,u,"entryNames",T),y=i(n,u,"chunkNames",T),S=i(n,u,"assetNames",T),A=i(n,u,"inject",G),R=i(n,u,"banner",Z),O=i(n,u,"footer",Z),P=i(n,u,"entryPoints",Ye),B=i(n,u,"absWorkingDir",T),F=i(n,u,"stdin",Z),N=(g=i(n,u,"write",z))!=null?g:m,X=i(n,u,"allowOverwrite",z),q=i(n,u,"mangleCache",Z);if(u.plugins=!0,Y(n,u,`in ${t}() call`),$&&c.push(`--sourcemap${$===!0?"":`=${$}`}`),L&&c.push("--bundle"),X&&c.push("--allow-overwrite"),D&&c.push("--splitting"),M&&c.push("--preserve-symlinks"),j&&c.push("--metafile"),C&&c.push(`--outfile=${C}`),k&&c.push(`--outdir=${k}`),V&&c.push(`--outbase=${V}`),E&&c.push(`--tsconfig=${E}`),_&&c.push(`--packages=${_}`),h){let b=[];for(let W of h){if(H(W,"resolve extension"),W.indexOf(",")>=0)throw new Error(`Invalid resolve extension: ${W}`);b.push(W)}c.push(`--resolve-extensions=${b.join(",")}`)}if(l&&c.push(`--public-path=${l}`),p&&c.push(`--entry-names=${p}`),y&&c.push(`--chunk-names=${y}`),S&&c.push(`--asset-names=${S}`),s){let b=[];for(let W of s){if(H(W,"main field"),W.indexOf(",")>=0)throw new Error(`Invalid main field: ${W}`);b.push(W)}c.push(`--main-fields=${b.join(",")}`)}if(f){let b=[];for(let W of f){if(H(W,"condition"),W.indexOf(",")>=0)throw new Error(`Invalid condition: ${W}`);b.push(W)}c.push(`--conditions=${b.join(",")}`)}if(w)for(let b of w)c.push(`--external:${H(b,"external")}`);if(x)for(let b in x){if(b.indexOf("=")>=0)throw new Error(`Invalid package name in alias: ${b}`);c.push(`--alias:${b}=${H(x[b],"alias",b)}`)}if(R)for(let b in R){if(b.indexOf("=")>=0)throw new Error(`Invalid banner file type: ${b}`);c.push(`--banner:${b}=${H(R[b],"banner",b)}`)}if(O)for(let b in O){if(b.indexOf("=")>=0)throw new Error(`Invalid footer file type: ${b}`);c.push(`--footer:${b}=${H(O[b],"footer",b)}`)}if(A)for(let b of A)c.push(`--inject:${H(b,"inject")}`);if(d)for(let b in d){if(b.indexOf("=")>=0)throw new Error(`Invalid loader extension: ${b}`);c.push(`--loader:${b}=${H(d[b],"loader",b)}`)}if(e)for(let b in e){if(b.indexOf("=")>=0)throw new Error(`Invalid out extension: ${b}`);c.push(`--out-extension:${b}=${H(e[b],"out extension",b)}`)}if(P)if(Array.isArray(P))for(let b=0,W=P.length;b<W;b++){let K=P[b];if(typeof K=="object"&&K!==null){let te=Object.create(null),Q=i(K,te,"in",T),ce=i(K,te,"out",T);if(Y(K,te,"in entry point at index "+b),Q===void 0)throw new Error('Missing property "in" for entry point at index '+b);if(ce===void 0)throw new Error('Missing property "out" for entry point at index '+b);v.push([ce,Q])}else v.push(["",H(K,"entry point at index "+b)])}else for(let b in P)v.push([b,H(P[b],"entry point",b)]);if(F){let b=Object.create(null),W=i(F,b,"contents",Re),K=i(F,b,"resolveDir",T),te=i(F,b,"sourcefile",T),Q=i(F,b,"loader",T);Y(F,b,'in "stdin" object'),te&&c.push(`--sourcefile=${te}`),Q&&c.push(`--loader=${Q}`),K&&(U=K),typeof W=="string"?I=ee(W):W instanceof Uint8Array&&(I=W)}let le=[];if(o)for(let b of o)b+="",le.push(b);return{entries:v,flags:c,write:N,stdinContents:I,stdinResolveDir:U,absWorkingDir:B,nodePaths:le,mangleCache:Ue(q)}}function tt(t,n,r,a){let m=[],g=Object.create(null);ye(m,n,g,r,a),Ie(m,n,g);let c=i(n,g,"sourcemap",De),v=i(n,g,"sourcefile",T),u=i(n,g,"loader",T),I=i(n,g,"banner",T),U=i(n,g,"footer",T),$=i(n,g,"mangleCache",Z);return Y(n,g,`in ${t}() call`),c&&m.push(`--sourcemap=${c===!0?"external":c}`),v&&m.push(`--sourcefile=${v}`),u&&m.push(`--loader=${u}`),I&&m.push(`--banner=${I}`),U&&m.push(`--footer=${U}`),{flags:m,mangleCache:Ue($)}}function nt(t){const n={},r={didClose:!1,reason:""};let a={},m=0,g=0,c=new Uint8Array(16*1024),v=0,u=E=>{let h=v+E.length;if(h>c.length){let s=new Uint8Array(h*2);s.set(c),c=s}c.set(E,v),v+=E.length;let o=0;for(;o+4<=v;){let s=Ce(c,o);if(o+4+s>v)break;o+=4,M(c.subarray(o,o+s)),o+=s}o>0&&(c.copyWithin(0,o,v),v-=o)},I=E=>{r.didClose=!0,E&&(r.reason=": "+(E.message||E));const h="The service was stopped"+r.reason;for(let o in a)a[o](h,null);a={}},U=(E,h,o)=>{if(r.didClose)return o("The service is no longer running"+r.reason,null);let s=m++;a[s]=(f,w)=>{try{o(f,w)}finally{E&&E.unref()}},E&&E.ref(),t.writeToStdin(ke({id:s,isRequest:!0,value:h}))},$=(E,h)=>{if(r.didClose)throw new Error("The service is no longer running"+r.reason);t.writeToStdin(ke({id:E,isRequest:!1,value:h}))},L=async(E,h)=>{try{if(h.command==="ping"){$(E,{});return}if(typeof h.key=="number"){const o=n[h.key];if(!o)return;const s=o[h.command];if(s){await s(E,h);return}}throw new Error("Invalid command: "+h.command)}catch(o){const s=[se(o,t,null,void 0,"")];try{$(E,{errors:s})}catch{}}},D=!0,M=E=>{if(D){D=!1;let o=String.fromCharCode(...E);if(o!=="0.24.2")throw new Error(`Cannot start service: Host version "0.24.2" does not match binary version ${J(o)}`);return}let h=Je(E);if(h.isRequest)L(h.id,h.value);else{let o=a[h.id];delete a[h.id],h.value.error?o(h.value.error,{}):o(null,h.value)}};return{readFromStdout:u,afterClose:I,service:{buildOrContext:({callName:E,refs:h,options:o,isTTY:s,defaultWD:f,callback:w})=>{let _=0;const x=g++,d={},e={ref(){++_===1&&h&&h.ref()},unref(){--_===0&&(delete n[x],h&&h.unref())}};n[x]=d,e.ref(),rt(E,x,U,$,e,t,d,o,s,f,(l,p)=>{try{w(l,p)}finally{e.unref()}})},transform:({callName:E,refs:h,input:o,options:s,isTTY:f,fs:w,callback:_})=>{const x=Me();let d=e=>{try{if(typeof o!="string"&&!(o instanceof Uint8Array))throw new Error('The input to "transform" must be a string or a Uint8Array');let{flags:l,mangleCache:p}=tt(E,s,f,_e),y={command:"transform",flags:l,inputFS:e!==null,input:e!==null?ee(e):typeof o=="string"?ee(o):o};p&&(y.mangleCache=p),U(h,y,(S,A)=>{if(S)return _(new Error(S),null);let R=ae(A.errors,x),O=ae(A.warnings,x),P=1,B=()=>{if(--P===0){let F={warnings:O,code:A.code,map:A.map,mangleCache:void 0,legalComments:void 0};"legalComments"in A&&(F.legalComments=A?.legalComments),A.mangleCache&&(F.mangleCache=A?.mangleCache),_(null,F)}};if(R.length>0)return _(ue("Transform failed",R,O),null);A.codeFS&&(P++,w.readFile(A.code,(F,N)=>{F!==null?_(F,null):(A.code=N,B())})),A.mapFS&&(P++,w.readFile(A.map,(F,N)=>{F!==null?_(F,null):(A.map=N,B())})),B()})}catch(l){let p=[];try{ye(p,s,{},f,_e)}catch{}const y=se(l,t,x,void 0,"");U(h,{command:"error",flags:p,error:y},()=>{y.detail=x.load(y.detail),_(ue("Transform failed",[y],[]),null)})}};if((typeof o=="string"||o instanceof Uint8Array)&&o.length>1024*1024){let e=d;d=()=>w.writeFile(o,e)}d(null)},formatMessages:({callName:E,refs:h,messages:o,options:s,callback:f})=>{if(!s)throw new Error(`Missing second argument in ${E}() call`);let w={},_=i(s,w,"kind",T),x=i(s,w,"color",z),d=i(s,w,"terminalWidth",ie);if(Y(s,w,`in ${E}() call`),_===void 0)throw new Error(`Missing "kind" in ${E}() call`);if(_!=="error"&&_!=="warning")throw new Error(`Expected "kind" to be "error" or "warning" in ${E}() call`);let e={command:"format-msgs",messages:ne(o,"messages",null,"",d),isWarning:_==="warning"};x!==void 0&&(e.color=x),d!==void 0&&(e.terminalWidth=d),U(h,e,(l,p)=>{if(l)return f(new Error(l),null);f(null,p.messages)})},analyzeMetafile:({callName:E,refs:h,metafile:o,options:s,callback:f})=>{s===void 0&&(s={});let w={},_=i(s,w,"color",z),x=i(s,w,"verbose",z);Y(s,w,`in ${E}() call`);let d={command:"analyze-metafile",metafile:o};_!==void 0&&(d.color=_),x!==void 0&&(d.verbose=x),U(h,d,(e,l)=>{if(e)return f(new Error(e),null);f(null,l.result)})}}}}function rt(t,n,r,a,m,g,c,v,u,I,U){const $=Me(),L=t==="context",D=(C,k)=>{const V=[];try{ye(V,v,{},u,Ee)}catch{}const E=se(C,g,$,void 0,k);r(m,{command:"error",flags:V,error:E},()=>{E.detail=$.load(E.detail),U(ue(L?"Context failed":"Build failed",[E],[]),null)})};let M;if(typeof v=="object"){const C=v.plugins;if(C!==void 0){if(!Array.isArray(C))return D(new Error('"plugins" must be an array'),"");M=C}}if(M&&M.length>0){if(g.isSync)return D(new Error("Cannot use plugins in synchronous API calls"),"");st(n,r,a,m,g,c,v,M,$).then(C=>{if(!C.ok)return D(C.error,C.pluginName);try{j(C.requestPlugins,C.runOnEndCallbacks,C.scheduleOnDisposeCallbacks)}catch(k){D(k,"")}},C=>D(C,""));return}try{j(null,(C,k)=>k([],[]),()=>{})}catch(C){D(C,"")}function j(C,k,V){const E=g.hasFS,{entries:h,flags:o,write:s,stdinContents:f,stdinResolveDir:w,absWorkingDir:_,nodePaths:x,mangleCache:d}=et(t,v,u,Ee,E);if(s&&!g.hasFS)throw new Error('The "write" option is unavailable in this environment');const e={command:"build",key:n,entries:h,flags:o,write:s,stdinContents:f,stdinResolveDir:w,absWorkingDir:_||I,nodePaths:x,context:L};C&&(e.plugins=C),d&&(e.mangleCache=d);const l=(S,A)=>{const R={errors:ae(S.errors,$),warnings:ae(S.warnings,$),outputFiles:void 0,metafile:void 0,mangleCache:void 0},O=R.errors.slice(),P=R.warnings.slice();S.outputFiles&&(R.outputFiles=S.outputFiles.map(lt)),S.metafile&&(R.metafile=JSON.parse(S.metafile)),S.mangleCache&&(R.mangleCache=S.mangleCache),S.writeToStdout!==void 0&&console.log(oe(S.writeToStdout).replace(/\n$/,"")),k(R,(B,F)=>{if(O.length>0||B.length>0){const N=ue("Build failed",O.concat(B),P.concat(F));return A(N,null,B,F)}A(null,R,B,F)})};let p,y;L&&(c["on-end"]=(S,A)=>new Promise(R=>{l(A,(O,P,B,F)=>{const N={errors:B,warnings:F};y&&y(O,P),p=void 0,y=void 0,a(S,N),R()})})),r(m,e,(S,A)=>{if(S)return U(new Error(S),null);if(!L)return l(A,(P,B)=>(V(),U(P,B)));if(A.errors.length>0)return U(ue("Context failed",A.errors,A.warnings),null);let R=!1;const O={rebuild:()=>(p||(p=new Promise((P,B)=>{let F;y=(X,q)=>{F||(F=()=>X?B(X):P(q))};const N=()=>{r(m,{command:"rebuild",key:n},(q,le)=>{q?B(new Error(q)):F?F():N()})};N()})),p),watch:(P={})=>new Promise((B,F)=>{if(!g.hasFS)throw new Error('Cannot use the "watch" API in this environment');Y(P,{},"in watch() call"),r(m,{command:"watch",key:n},q=>{q?F(new Error(q)):B(void 0)})}),serve:(P={})=>new Promise((B,F)=>{if(!g.hasFS)throw new Error('Cannot use the "serve" API in this environment');const N={},X=i(P,N,"port",ie),q=i(P,N,"host",T),le=i(P,N,"servedir",T),b=i(P,N,"keyfile",T),W=i(P,N,"certfile",T),K=i(P,N,"fallback",T),te=i(P,N,"onRequest",Ae);Y(P,N,"in serve() call");const Q={command:"serve",key:n,onRequest:!!te};X!==void 0&&(Q.port=X),q!==void 0&&(Q.host=q),le!==void 0&&(Q.servedir=le),b!==void 0&&(Q.keyfile=b),W!==void 0&&(Q.certfile=W),K!==void 0&&(Q.fallback=K),r(m,Q,(ce,Be)=>{if(ce)return F(new Error(ce));te&&(c["serve-request"]=(We,ze)=>{te(ze.args),a(We,{})}),B(Be)})}),cancel:()=>new Promise(P=>{if(R)return P();r(m,{command:"cancel",key:n},()=>{P()})}),dispose:()=>new Promise(P=>{if(R)return P();R=!0,r(m,{command:"dispose",key:n},()=>{P(),V(),m.unref()})})};m.ref(),U(null,O)})}}var st=async(t,n,r,a,m,g,c,v,u)=>{let I=[],U=[],$={},L={},D=[],M=0,j=0,C=[],k=!1;v=[...v];for(let h of v){let o={};if(typeof h!="object")throw new Error(`Plugin at index ${j} must be an object`);const s=i(h,o,"name",T);if(typeof s!="string"||s==="")throw new Error(`Plugin at index ${j} is missing a name`);try{let f=i(h,o,"setup",Ae);if(typeof f!="function")throw new Error("Plugin is missing a setup function");Y(h,o,`on plugin ${J(s)}`);let w={name:s,onStart:!1,onEnd:!1,onResolve:[],onLoad:[]};j++;let x=f({initialOptions:c,resolve:(d,e={})=>{if(!k)throw new Error('Cannot call "resolve" before plugin setup has completed');if(typeof d!="string")throw new Error("The path to resolve must be a string");let l=Object.create(null),p=i(e,l,"pluginName",T),y=i(e,l,"importer",T),S=i(e,l,"namespace",T),A=i(e,l,"resolveDir",T),R=i(e,l,"kind",T),O=i(e,l,"pluginData",me),P=i(e,l,"with",Z);return Y(e,l,"in resolve() call"),new Promise((B,F)=>{const N={command:"resolve",path:d,key:t,pluginName:s};if(p!=null&&(N.pluginName=p),y!=null&&(N.importer=y),S!=null&&(N.namespace=S),A!=null&&(N.resolveDir=A),R!=null)N.kind=R;else throw new Error('Must specify "kind" when calling "resolve"');O!=null&&(N.pluginData=u.store(O)),P!=null&&(N.with=it(P,"with")),n(a,N,(X,q)=>{X!==null?F(new Error(X)):B({errors:ae(q.errors,u),warnings:ae(q.warnings,u),path:q.path,external:q.external,sideEffects:q.sideEffects,namespace:q.namespace,suffix:q.suffix,pluginData:u.load(q.pluginData)})})})},onStart(d){let e='This error came from the "onStart" callback registered here:',l=de(new Error(e),m,"onStart");I.push({name:s,callback:d,note:l}),w.onStart=!0},onEnd(d){let e='This error came from the "onEnd" callback registered here:',l=de(new Error(e),m,"onEnd");U.push({name:s,callback:d,note:l}),w.onEnd=!0},onResolve(d,e){let l='This error came from the "onResolve" callback registered here:',p=de(new Error(l),m,"onResolve"),y={},S=i(d,y,"filter",ge),A=i(d,y,"namespace",T);if(Y(d,y,`in onResolve() call for plugin ${J(s)}`),S==null)throw new Error("onResolve() call is missing a filter");let R=M++;$[R]={name:s,callback:e,note:p},w.onResolve.push({id:R,filter:S.source,namespace:A||""})},onLoad(d,e){let l='This error came from the "onLoad" callback registered here:',p=de(new Error(l),m,"onLoad"),y={},S=i(d,y,"filter",ge),A=i(d,y,"namespace",T);if(Y(d,y,`in onLoad() call for plugin ${J(s)}`),S==null)throw new Error("onLoad() call is missing a filter");let R=M++;L[R]={name:s,callback:e,note:p},w.onLoad.push({id:R,filter:S.source,namespace:A||""})},onDispose(d){D.push(d)},esbuild:m.esbuild});x&&await x,C.push(w)}catch(f){return{ok:!1,error:f,pluginName:s}}}g["on-start"]=async(h,o)=>{u.clear();let s={errors:[],warnings:[]};await Promise.all(I.map(async({name:f,callback:w,note:_})=>{try{let x=await w();if(x!=null){if(typeof x!="object")throw new Error(`Expected onStart() callback in plugin ${J(f)} to return an object`);let d={},e=i(x,d,"errors",G),l=i(x,d,"warnings",G);Y(x,d,`from onStart() callback in plugin ${J(f)}`),e!=null&&s.errors.push(...ne(e,"errors",u,f,void 0)),l!=null&&s.warnings.push(...ne(l,"warnings",u,f,void 0))}}catch(x){s.errors.push(se(x,m,u,_&&_(),f))}})),r(h,s)},g["on-resolve"]=async(h,o)=>{let s={},f="",w,_;for(let x of o.ids)try{({name:f,callback:w,note:_}=$[x]);let d=await w({path:o.path,importer:o.importer,namespace:o.namespace,resolveDir:o.resolveDir,kind:o.kind,pluginData:u.load(o.pluginData),with:o.with});if(d!=null){if(typeof d!="object")throw new Error(`Expected onResolve() callback in plugin ${J(f)} to return an object`);let e={},l=i(d,e,"pluginName",T),p=i(d,e,"path",T),y=i(d,e,"namespace",T),S=i(d,e,"suffix",T),A=i(d,e,"external",z),R=i(d,e,"sideEffects",z),O=i(d,e,"pluginData",me),P=i(d,e,"errors",G),B=i(d,e,"warnings",G),F=i(d,e,"watchFiles",G),N=i(d,e,"watchDirs",G);Y(d,e,`from onResolve() callback in plugin ${J(f)}`),s.id=x,l!=null&&(s.pluginName=l),p!=null&&(s.path=p),y!=null&&(s.namespace=y),S!=null&&(s.suffix=S),A!=null&&(s.external=A),R!=null&&(s.sideEffects=R),O!=null&&(s.pluginData=u.store(O)),P!=null&&(s.errors=ne(P,"errors",u,f,void 0)),B!=null&&(s.warnings=ne(B,"warnings",u,f,void 0)),F!=null&&(s.watchFiles=he(F,"watchFiles")),N!=null&&(s.watchDirs=he(N,"watchDirs"));break}}catch(d){s={id:x,errors:[se(d,m,u,_&&_(),f)]};break}r(h,s)},g["on-load"]=async(h,o)=>{let s={},f="",w,_;for(let x of o.ids)try{({name:f,callback:w,note:_}=L[x]);let d=await w({path:o.path,namespace:o.namespace,suffix:o.suffix,pluginData:u.load(o.pluginData),with:o.with});if(d!=null){if(typeof d!="object")throw new Error(`Expected onLoad() callback in plugin ${J(f)} to return an object`);let e={},l=i(d,e,"pluginName",T),p=i(d,e,"contents",Re),y=i(d,e,"resolveDir",T),S=i(d,e,"pluginData",me),A=i(d,e,"loader",T),R=i(d,e,"errors",G),O=i(d,e,"warnings",G),P=i(d,e,"watchFiles",G),B=i(d,e,"watchDirs",G);Y(d,e,`from onLoad() callback in plugin ${J(f)}`),s.id=x,l!=null&&(s.pluginName=l),p instanceof Uint8Array?s.contents=p:p!=null&&(s.contents=ee(p)),y!=null&&(s.resolveDir=y),S!=null&&(s.pluginData=u.store(S)),A!=null&&(s.loader=A),R!=null&&(s.errors=ne(R,"errors",u,f,void 0)),O!=null&&(s.warnings=ne(O,"warnings",u,f,void 0)),P!=null&&(s.watchFiles=he(P,"watchFiles")),B!=null&&(s.watchDirs=he(B,"watchDirs"));break}}catch(d){s={id:x,errors:[se(d,m,u,_&&_(),f)]};break}r(h,s)};let V=(h,o)=>o([],[]);U.length>0&&(V=(h,o)=>{(async()=>{const s=[],f=[];for(const{name:w,callback:_,note:x}of U){let d,e;try{const l=await _(h);if(l!=null){if(typeof l!="object")throw new Error(`Expected onEnd() callback in plugin ${J(w)} to return an object`);let p={},y=i(l,p,"errors",G),S=i(l,p,"warnings",G);Y(l,p,`from onEnd() callback in plugin ${J(w)}`),y!=null&&(d=ne(y,"errors",u,w,void 0)),S!=null&&(e=ne(S,"warnings",u,w,void 0))}}catch(l){d=[se(l,m,u,x&&x(),w)]}if(d){s.push(...d);try{h.errors.push(...d)}catch{}}if(e){f.push(...e);try{h.warnings.push(...e)}catch{}}}o(s,f)})()});let E=()=>{for(const h of D)setTimeout(()=>h(),0)};return k=!0,{ok:!0,requestPlugins:C,runOnEndCallbacks:V,scheduleOnDisposeCallbacks:E}};function Me(){const t=new Map;let n=0;return{clear(){t.clear()},load(r){return t.get(r)},store(r){if(r===void 0)return-1;const a=n++;return t.set(a,r),a}}}function de(t,n,r){let a,m=!1;return()=>{if(m)return a;m=!0;try{let g=(t.stack+"").split(`
`);g.splice(1,1);let c=Fe(n,g,r);if(c)return a={text:t.message,location:c},a}catch{}}}function se(t,n,r,a,m){let g="Internal error",c=null;try{g=(t&&t.message||t)+""}catch{}try{c=Fe(n,(t.stack+"").split(`
`),"")}catch{}return{id:"",pluginName:m,text:g,location:c,notes:a?[a]:[],detail:r?r.store(t):-1}}function Fe(t,n,r){let a="    at ";if(t.readFileSync&&!n[0].startsWith(a)&&n[1].startsWith(a))for(let m=1;m<n.length;m++){let g=n[m];if(g.startsWith(a))for(g=g.slice(a.length);;){let c=/^(?:new |async )?\S+ \((.*)\)$/.exec(g);if(c){g=c[1];continue}if(c=/^eval at \S+ \((.*)\)(?:, \S+:\d+:\d+)?$/.exec(g),c){g=c[1];continue}if(c=/^(\S+):(\d+):(\d+)$/.exec(g),c){let v;try{v=t.readFileSync(c[1],"utf8")}catch{break}let u=v.split(/\r\n|\r|\n|\u2028|\u2029/)[+c[2]-1]||"",I=+c[3]-1,U=u.slice(I,I+r.length)===r?r.length:0;return{file:c[1],namespace:"file",line:+c[2],column:ee(u.slice(0,I)).length,length:ee(u.slice(I,I+U)).length,lineText:u+`
`+n.slice(1).join(`
`),suggestion:""}}break}}return null}function ue(t,n,r){let a=5;t+=n.length<1?"":` with ${n.length} error${n.length<2?"":"s"}:`+n.slice(0,a+1).map((g,c)=>{if(c===a)return`
...`;if(!g.location)return`
error: ${g.text}`;let{file:v,line:u,column:I}=g.location,U=g.pluginName?`[plugin: ${g.pluginName}] `:"";return`
${v}:${u}:${I}: ERROR: ${U}${g.text}`}).join("");let m=new Error(t);for(const[g,c]of[["errors",n],["warnings",r]])Object.defineProperty(m,g,{configurable:!0,enumerable:!0,get:()=>c,set:v=>Object.defineProperty(m,g,{configurable:!0,enumerable:!0,value:v})});return m}function ae(t,n){for(const r of t)r.detail=n.load(r.detail);return t}function $e(t,n,r){if(t==null)return null;let a={},m=i(t,a,"file",T),g=i(t,a,"namespace",T),c=i(t,a,"line",ie),v=i(t,a,"column",ie),u=i(t,a,"length",ie),I=i(t,a,"lineText",T),U=i(t,a,"suggestion",T);if(Y(t,a,n),I){const $=I.slice(0,(v&&v>0?v:0)+(u&&u>0?u:0)+(r&&r>0?r:80));!/[\x7F-\uFFFF]/.test($)&&!/\n/.test(I)&&(I=$)}return{file:m||"",namespace:g||"",line:c||0,column:v||0,length:u||0,lineText:I||"",suggestion:U||""}}function ne(t,n,r,a,m){let g=[],c=0;for(const v of t){let u={},I=i(v,u,"id",T),U=i(v,u,"pluginName",T),$=i(v,u,"text",T),L=i(v,u,"location",Te),D=i(v,u,"notes",G),M=i(v,u,"detail",me),j=`in element ${c} of "${n}"`;Y(v,u,j);let C=[];if(D)for(const k of D){let V={},E=i(k,V,"text",T),h=i(k,V,"location",Te);Y(k,V,j),C.push({text:E||"",location:$e(h,j,m)})}g.push({id:I||"",pluginName:U||a,text:$||"",location:$e(L,j,m),notes:C,detail:r?r.store(M):-1}),c++}return g}function he(t,n){const r=[];for(const a of t){if(typeof a!="string")throw new Error(`${J(n)} must be an array of strings`);r.push(a)}return r}function it(t,n){const r=Object.create(null);for(const a in t){const m=t[a];if(typeof m!="string")throw new Error(`key ${J(a)} in object ${J(n)} must be a string`);r[a]=m}return r}function lt({path:t,contents:n,hash:r}){let a=null;return{path:t,contents:n,hash:r,get text(){const m=this.contents;return(a===null||m!==n)&&(n=m,a=oe(m)),a}}}var ot="0.24.2",at=t=>fe().build(t),ct=t=>fe().context(t),Ne=(t,n)=>fe().transform(t,n),ut=(t,n)=>fe().formatMessages(t,n),ft=(t,n)=>fe().analyzeMetafile(t,n),dt=()=>{throw new Error('The "buildSync" API only works in node')},ht=()=>{throw new Error('The "transformSync" API only works in node')},mt=()=>{throw new Error('The "formatMessagesSync" API only works in node')},gt=()=>{throw new Error('The "analyzeMetafileSync" API only works in node')},pt=()=>(pe&&pe(),Promise.resolve()),re,pe,we,fe=()=>{if(we)return we;throw re?new Error('You need to wait for the promise returned from "initialize" to be resolved before calling this'):new Error('You need to call "initialize" before calling this')},Le=t=>{t=Ze(t||{});let n=t.wasmURL,r=t.wasmModule,a=t.worker!==!1;if(!n&&!r)throw new Error('Must provide either the "wasmURL" option or the "wasmModule" option');if(re)throw new Error('Cannot call "initialize" more than once');return re=wt(n||"",r,a),re.catch(()=>{re=void 0}),re},wt=async(t,n,r)=>{let a,m;const g=new Promise($=>m=$);if(r){let $=new Blob([`onmessage=((postMessage) => {
      // Copyright 2018 The Go Authors. All rights reserved.
      // Use of this source code is governed by a BSD-style
      // license that can be found in the LICENSE file.
      let onmessage;
      let globalThis = {};
      for (let o = self; o; o = Object.getPrototypeOf(o))
        for (let k of Object.getOwnPropertyNames(o))
          if (!(k in globalThis))
            Object.defineProperty(globalThis, k, { get: () => self[k] });
      "use strict";
      (() => {
        const enosys = () => {
          const err = new Error("not implemented");
          err.code = "ENOSYS";
          return err;
        };
        if (!globalThis.fs) {
          let outputBuf = "";
          globalThis.fs = {
            constants: { O_WRONLY: -1, O_RDWR: -1, O_CREAT: -1, O_TRUNC: -1, O_APPEND: -1, O_EXCL: -1 },
            // unused
            writeSync(fd, buf) {
              outputBuf += decoder.decode(buf);
              const nl = outputBuf.lastIndexOf("\\n");
              if (nl != -1) {
                console.log(outputBuf.substring(0, nl));
                outputBuf = outputBuf.substring(nl + 1);
              }
              return buf.length;
            },
            write(fd, buf, offset, length, position, callback) {
              if (offset !== 0 || length !== buf.length || position !== null) {
                callback(enosys());
                return;
              }
              const n = this.writeSync(fd, buf);
              callback(null, n);
            },
            chmod(path, mode, callback) {
              callback(enosys());
            },
            chown(path, uid, gid, callback) {
              callback(enosys());
            },
            close(fd, callback) {
              callback(enosys());
            },
            fchmod(fd, mode, callback) {
              callback(enosys());
            },
            fchown(fd, uid, gid, callback) {
              callback(enosys());
            },
            fstat(fd, callback) {
              callback(enosys());
            },
            fsync(fd, callback) {
              callback(null);
            },
            ftruncate(fd, length, callback) {
              callback(enosys());
            },
            lchown(path, uid, gid, callback) {
              callback(enosys());
            },
            link(path, link, callback) {
              callback(enosys());
            },
            lstat(path, callback) {
              callback(enosys());
            },
            mkdir(path, perm, callback) {
              callback(enosys());
            },
            open(path, flags, mode, callback) {
              callback(enosys());
            },
            read(fd, buffer, offset, length, position, callback) {
              callback(enosys());
            },
            readdir(path, callback) {
              callback(enosys());
            },
            readlink(path, callback) {
              callback(enosys());
            },
            rename(from, to, callback) {
              callback(enosys());
            },
            rmdir(path, callback) {
              callback(enosys());
            },
            stat(path, callback) {
              callback(enosys());
            },
            symlink(path, link, callback) {
              callback(enosys());
            },
            truncate(path, length, callback) {
              callback(enosys());
            },
            unlink(path, callback) {
              callback(enosys());
            },
            utimes(path, atime, mtime, callback) {
              callback(enosys());
            }
          };
        }
        if (!globalThis.process) {
          globalThis.process = {
            getuid() {
              return -1;
            },
            getgid() {
              return -1;
            },
            geteuid() {
              return -1;
            },
            getegid() {
              return -1;
            },
            getgroups() {
              throw enosys();
            },
            pid: -1,
            ppid: -1,
            umask() {
              throw enosys();
            },
            cwd() {
              throw enosys();
            },
            chdir() {
              throw enosys();
            }
          };
        }
        if (!globalThis.crypto) {
          throw new Error("globalThis.crypto is not available, polyfill required (crypto.getRandomValues only)");
        }
        if (!globalThis.performance) {
          throw new Error("globalThis.performance is not available, polyfill required (performance.now only)");
        }
        if (!globalThis.TextEncoder) {
          throw new Error("globalThis.TextEncoder is not available, polyfill required");
        }
        if (!globalThis.TextDecoder) {
          throw new Error("globalThis.TextDecoder is not available, polyfill required");
        }
        const encoder = new TextEncoder("utf-8");
        const decoder = new TextDecoder("utf-8");
        globalThis.Go = class {
          constructor() {
            this.argv = ["js"];
            this.env = {};
            this.exit = (code) => {
              if (code !== 0) {
                console.warn("exit code:", code);
              }
            };
            this._exitPromise = new Promise((resolve) => {
              this._resolveExitPromise = resolve;
            });
            this._pendingEvent = null;
            this._scheduledTimeouts = /* @__PURE__ */ new Map();
            this._nextCallbackTimeoutID = 1;
            const setInt64 = (addr, v) => {
              this.mem.setUint32(addr + 0, v, true);
              this.mem.setUint32(addr + 4, Math.floor(v / 4294967296), true);
            };
            const setInt32 = (addr, v) => {
              this.mem.setUint32(addr + 0, v, true);
            };
            const getInt64 = (addr) => {
              const low = this.mem.getUint32(addr + 0, true);
              const high = this.mem.getInt32(addr + 4, true);
              return low + high * 4294967296;
            };
            const loadValue = (addr) => {
              const f = this.mem.getFloat64(addr, true);
              if (f === 0) {
                return void 0;
              }
              if (!isNaN(f)) {
                return f;
              }
              const id = this.mem.getUint32(addr, true);
              return this._values[id];
            };
            const storeValue = (addr, v) => {
              const nanHead = 2146959360;
              if (typeof v === "number" && v !== 0) {
                if (isNaN(v)) {
                  this.mem.setUint32(addr + 4, nanHead, true);
                  this.mem.setUint32(addr, 0, true);
                  return;
                }
                this.mem.setFloat64(addr, v, true);
                return;
              }
              if (v === void 0) {
                this.mem.setFloat64(addr, 0, true);
                return;
              }
              let id = this._ids.get(v);
              if (id === void 0) {
                id = this._idPool.pop();
                if (id === void 0) {
                  id = this._values.length;
                }
                this._values[id] = v;
                this._goRefCounts[id] = 0;
                this._ids.set(v, id);
              }
              this._goRefCounts[id]++;
              let typeFlag = 0;
              switch (typeof v) {
                case "object":
                  if (v !== null) {
                    typeFlag = 1;
                  }
                  break;
                case "string":
                  typeFlag = 2;
                  break;
                case "symbol":
                  typeFlag = 3;
                  break;
                case "function":
                  typeFlag = 4;
                  break;
              }
              this.mem.setUint32(addr + 4, nanHead | typeFlag, true);
              this.mem.setUint32(addr, id, true);
            };
            const loadSlice = (addr) => {
              const array = getInt64(addr + 0);
              const len = getInt64(addr + 8);
              return new Uint8Array(this._inst.exports.mem.buffer, array, len);
            };
            const loadSliceOfValues = (addr) => {
              const array = getInt64(addr + 0);
              const len = getInt64(addr + 8);
              const a = new Array(len);
              for (let i = 0; i < len; i++) {
                a[i] = loadValue(array + i * 8);
              }
              return a;
            };
            const loadString = (addr) => {
              const saddr = getInt64(addr + 0);
              const len = getInt64(addr + 8);
              return decoder.decode(new DataView(this._inst.exports.mem.buffer, saddr, len));
            };
            const timeOrigin = Date.now() - performance.now();
            this.importObject = {
              _gotest: {
                add: (a, b) => a + b
              },
              gojs: {
                // Go's SP does not change as long as no Go code is running. Some operations (e.g. calls, getters and setters)
                // may synchronously trigger a Go event handler. This makes Go code get executed in the middle of the imported
                // function. A goroutine can switch to a new stack if the current stack is too small (see morestack function).
                // This changes the SP, thus we have to update the SP used by the imported function.
                // func wasmExit(code int32)
                "runtime.wasmExit": (sp) => {
                  sp >>>= 0;
                  const code = this.mem.getInt32(sp + 8, true);
                  this.exited = true;
                  delete this._inst;
                  delete this._values;
                  delete this._goRefCounts;
                  delete this._ids;
                  delete this._idPool;
                  this.exit(code);
                },
                // func wasmWrite(fd uintptr, p unsafe.Pointer, n int32)
                "runtime.wasmWrite": (sp) => {
                  sp >>>= 0;
                  const fd = getInt64(sp + 8);
                  const p = getInt64(sp + 16);
                  const n = this.mem.getInt32(sp + 24, true);
                  globalThis.fs.writeSync(fd, new Uint8Array(this._inst.exports.mem.buffer, p, n));
                },
                // func resetMemoryDataView()
                "runtime.resetMemoryDataView": (sp) => {
                  sp >>>= 0;
                  this.mem = new DataView(this._inst.exports.mem.buffer);
                },
                // func nanotime1() int64
                "runtime.nanotime1": (sp) => {
                  sp >>>= 0;
                  setInt64(sp + 8, (timeOrigin + performance.now()) * 1e6);
                },
                // func walltime() (sec int64, nsec int32)
                "runtime.walltime": (sp) => {
                  sp >>>= 0;
                  const msec = (/* @__PURE__ */ new Date()).getTime();
                  setInt64(sp + 8, msec / 1e3);
                  this.mem.setInt32(sp + 16, msec % 1e3 * 1e6, true);
                },
                // func scheduleTimeoutEvent(delay int64) int32
                "runtime.scheduleTimeoutEvent": (sp) => {
                  sp >>>= 0;
                  const id = this._nextCallbackTimeoutID;
                  this._nextCallbackTimeoutID++;
                  this._scheduledTimeouts.set(id, setTimeout(
                    () => {
                      this._resume();
                      while (this._scheduledTimeouts.has(id)) {
                        console.warn("scheduleTimeoutEvent: missed timeout event");
                        this._resume();
                      }
                    },
                    getInt64(sp + 8)
                  ));
                  this.mem.setInt32(sp + 16, id, true);
                },
                // func clearTimeoutEvent(id int32)
                "runtime.clearTimeoutEvent": (sp) => {
                  sp >>>= 0;
                  const id = this.mem.getInt32(sp + 8, true);
                  clearTimeout(this._scheduledTimeouts.get(id));
                  this._scheduledTimeouts.delete(id);
                },
                // func getRandomData(r []byte)
                "runtime.getRandomData": (sp) => {
                  sp >>>= 0;
                  crypto.getRandomValues(loadSlice(sp + 8));
                },
                // func finalizeRef(v ref)
                "syscall/js.finalizeRef": (sp) => {
                  sp >>>= 0;
                  const id = this.mem.getUint32(sp + 8, true);
                  this._goRefCounts[id]--;
                  if (this._goRefCounts[id] === 0) {
                    const v = this._values[id];
                    this._values[id] = null;
                    this._ids.delete(v);
                    this._idPool.push(id);
                  }
                },
                // func stringVal(value string) ref
                "syscall/js.stringVal": (sp) => {
                  sp >>>= 0;
                  storeValue(sp + 24, loadString(sp + 8));
                },
                // func valueGet(v ref, p string) ref
                "syscall/js.valueGet": (sp) => {
                  sp >>>= 0;
                  const result = Reflect.get(loadValue(sp + 8), loadString(sp + 16));
                  sp = this._inst.exports.getsp() >>> 0;
                  storeValue(sp + 32, result);
                },
                // func valueSet(v ref, p string, x ref)
                "syscall/js.valueSet": (sp) => {
                  sp >>>= 0;
                  Reflect.set(loadValue(sp + 8), loadString(sp + 16), loadValue(sp + 32));
                },
                // func valueDelete(v ref, p string)
                "syscall/js.valueDelete": (sp) => {
                  sp >>>= 0;
                  Reflect.deleteProperty(loadValue(sp + 8), loadString(sp + 16));
                },
                // func valueIndex(v ref, i int) ref
                "syscall/js.valueIndex": (sp) => {
                  sp >>>= 0;
                  storeValue(sp + 24, Reflect.get(loadValue(sp + 8), getInt64(sp + 16)));
                },
                // valueSetIndex(v ref, i int, x ref)
                "syscall/js.valueSetIndex": (sp) => {
                  sp >>>= 0;
                  Reflect.set(loadValue(sp + 8), getInt64(sp + 16), loadValue(sp + 24));
                },
                // func valueCall(v ref, m string, args []ref) (ref, bool)
                "syscall/js.valueCall": (sp) => {
                  sp >>>= 0;
                  try {
                    const v = loadValue(sp + 8);
                    const m = Reflect.get(v, loadString(sp + 16));
                    const args = loadSliceOfValues(sp + 32);
                    const result = Reflect.apply(m, v, args);
                    sp = this._inst.exports.getsp() >>> 0;
                    storeValue(sp + 56, result);
                    this.mem.setUint8(sp + 64, 1);
                  } catch (err) {
                    sp = this._inst.exports.getsp() >>> 0;
                    storeValue(sp + 56, err);
                    this.mem.setUint8(sp + 64, 0);
                  }
                },
                // func valueInvoke(v ref, args []ref) (ref, bool)
                "syscall/js.valueInvoke": (sp) => {
                  sp >>>= 0;
                  try {
                    const v = loadValue(sp + 8);
                    const args = loadSliceOfValues(sp + 16);
                    const result = Reflect.apply(v, void 0, args);
                    sp = this._inst.exports.getsp() >>> 0;
                    storeValue(sp + 40, result);
                    this.mem.setUint8(sp + 48, 1);
                  } catch (err) {
                    sp = this._inst.exports.getsp() >>> 0;
                    storeValue(sp + 40, err);
                    this.mem.setUint8(sp + 48, 0);
                  }
                },
                // func valueNew(v ref, args []ref) (ref, bool)
                "syscall/js.valueNew": (sp) => {
                  sp >>>= 0;
                  try {
                    const v = loadValue(sp + 8);
                    const args = loadSliceOfValues(sp + 16);
                    const result = Reflect.construct(v, args);
                    sp = this._inst.exports.getsp() >>> 0;
                    storeValue(sp + 40, result);
                    this.mem.setUint8(sp + 48, 1);
                  } catch (err) {
                    sp = this._inst.exports.getsp() >>> 0;
                    storeValue(sp + 40, err);
                    this.mem.setUint8(sp + 48, 0);
                  }
                },
                // func valueLength(v ref) int
                "syscall/js.valueLength": (sp) => {
                  sp >>>= 0;
                  setInt64(sp + 16, parseInt(loadValue(sp + 8).length));
                },
                // valuePrepareString(v ref) (ref, int)
                "syscall/js.valuePrepareString": (sp) => {
                  sp >>>= 0;
                  const str = encoder.encode(String(loadValue(sp + 8)));
                  storeValue(sp + 16, str);
                  setInt64(sp + 24, str.length);
                },
                // valueLoadString(v ref, b []byte)
                "syscall/js.valueLoadString": (sp) => {
                  sp >>>= 0;
                  const str = loadValue(sp + 8);
                  loadSlice(sp + 16).set(str);
                },
                // func valueInstanceOf(v ref, t ref) bool
                "syscall/js.valueInstanceOf": (sp) => {
                  sp >>>= 0;
                  this.mem.setUint8(sp + 24, loadValue(sp + 8) instanceof loadValue(sp + 16) ? 1 : 0);
                },
                // func copyBytesToGo(dst []byte, src ref) (int, bool)
                "syscall/js.copyBytesToGo": (sp) => {
                  sp >>>= 0;
                  const dst = loadSlice(sp + 8);
                  const src = loadValue(sp + 32);
                  if (!(src instanceof Uint8Array || src instanceof Uint8ClampedArray)) {
                    this.mem.setUint8(sp + 48, 0);
                    return;
                  }
                  const toCopy = src.subarray(0, dst.length);
                  dst.set(toCopy);
                  setInt64(sp + 40, toCopy.length);
                  this.mem.setUint8(sp + 48, 1);
                },
                // func copyBytesToJS(dst ref, src []byte) (int, bool)
                "syscall/js.copyBytesToJS": (sp) => {
                  sp >>>= 0;
                  const dst = loadValue(sp + 8);
                  const src = loadSlice(sp + 16);
                  if (!(dst instanceof Uint8Array || dst instanceof Uint8ClampedArray)) {
                    this.mem.setUint8(sp + 48, 0);
                    return;
                  }
                  const toCopy = src.subarray(0, dst.length);
                  dst.set(toCopy);
                  setInt64(sp + 40, toCopy.length);
                  this.mem.setUint8(sp + 48, 1);
                },
                "debug": (value) => {
                  console.log(value);
                }
              }
            };
          }
          async run(instance) {
            if (!(instance instanceof WebAssembly.Instance)) {
              throw new Error("Go.run: WebAssembly.Instance expected");
            }
            this._inst = instance;
            this.mem = new DataView(this._inst.exports.mem.buffer);
            this._values = [
              // JS values that Go currently has references to, indexed by reference id
              NaN,
              0,
              null,
              true,
              false,
              globalThis,
              this
            ];
            this._goRefCounts = new Array(this._values.length).fill(Infinity);
            this._ids = /* @__PURE__ */ new Map([
              // mapping from JS values to reference ids
              [0, 1],
              [null, 2],
              [true, 3],
              [false, 4],
              [globalThis, 5],
              [this, 6]
            ]);
            this._idPool = [];
            this.exited = false;
            let offset = 4096;
            const strPtr = (str) => {
              const ptr = offset;
              const bytes = encoder.encode(str + "\\0");
              new Uint8Array(this.mem.buffer, offset, bytes.length).set(bytes);
              offset += bytes.length;
              if (offset % 8 !== 0) {
                offset += 8 - offset % 8;
              }
              return ptr;
            };
            const argc = this.argv.length;
            const argvPtrs = [];
            this.argv.forEach((arg) => {
              argvPtrs.push(strPtr(arg));
            });
            argvPtrs.push(0);
            const keys = Object.keys(this.env).sort();
            keys.forEach((key) => {
              argvPtrs.push(strPtr(\`\${key}=\${this.env[key]}\`));
            });
            argvPtrs.push(0);
            const argv = offset;
            argvPtrs.forEach((ptr) => {
              this.mem.setUint32(offset, ptr, true);
              this.mem.setUint32(offset + 4, 0, true);
              offset += 8;
            });
            const wasmMinDataAddr = 4096 + 8192;
            if (offset >= wasmMinDataAddr) {
              throw new Error("total length of command line and environment variables exceeds limit");
            }
            this._inst.exports.run(argc, argv);
            if (this.exited) {
              this._resolveExitPromise();
            }
            await this._exitPromise;
          }
          _resume() {
            if (this.exited) {
              throw new Error("Go program has already exited");
            }
            this._inst.exports.resume();
            if (this.exited) {
              this._resolveExitPromise();
            }
          }
          _makeFuncWrapper(id) {
            const go = this;
            return function() {
              const event = { id, this: this, args: arguments };
              go._pendingEvent = event;
              go._resume();
              return event.result;
            };
          }
        };
      })();
      onmessage = ({ data: wasm }) => {
        let decoder = new TextDecoder();
        let fs = globalThis.fs;
        let stderr = "";
        fs.writeSync = (fd, buffer) => {
          if (fd === 1) {
            postMessage(buffer);
          } else if (fd === 2) {
            stderr += decoder.decode(buffer);
            let parts = stderr.split("\\n");
            if (parts.length > 1) console.log(parts.slice(0, -1).join("\\n"));
            stderr = parts[parts.length - 1];
          } else {
            throw new Error("Bad write");
          }
          return buffer.length;
        };
        let stdin = [];
        let resumeStdin;
        let stdinPos = 0;
        onmessage = ({ data }) => {
          if (data.length > 0) {
            stdin.push(data);
            if (resumeStdin) resumeStdin();
          }
          return go;
        };
        fs.read = (fd, buffer, offset, length, position, callback) => {
          if (fd !== 0 || offset !== 0 || length !== buffer.length || position !== null) {
            throw new Error("Bad read");
          }
          if (stdin.length === 0) {
            resumeStdin = () => fs.read(fd, buffer, offset, length, position, callback);
            return;
          }
          let first = stdin[0];
          let count = Math.max(0, Math.min(length, first.length - stdinPos));
          buffer.set(first.subarray(stdinPos, stdinPos + count), offset);
          stdinPos += count;
          if (stdinPos === first.length) {
            stdin.shift();
            stdinPos = 0;
          }
          callback(null, count);
        };
        let go = new globalThis.Go();
        go.argv = ["", \`--service=\${"0.24.2"}\`];
        tryToInstantiateModule(wasm, go).then(
          (instance) => {
            postMessage(null);
            go.run(instance);
          },
          (error) => {
            postMessage(error);
          }
        );
        return go;
      };
      async function tryToInstantiateModule(wasm, go) {
        if (wasm instanceof WebAssembly.Module) {
          return WebAssembly.instantiate(wasm, go.importObject);
        }
        const res = await fetch(wasm);
        if (!res.ok) throw new Error(\`Failed to download \${JSON.stringify(wasm)}\`);
        if ("instantiateStreaming" in WebAssembly && /^application\\/wasm($|;)/i.test(res.headers.get("Content-Type") || "")) {
          const result2 = await WebAssembly.instantiateStreaming(res, go.importObject);
          return result2.instance;
        }
        const bytes = await res.arrayBuffer();
        const result = await WebAssembly.instantiate(bytes, go.importObject);
        return result.instance;
      }
      return (m) => onmessage(m);
    })(postMessage)`],{type:"text/javascript"});a=new Worker(URL.createObjectURL($))}else{let $=(D=>{let M,j={};for(let k=self;k;k=Object.getPrototypeOf(k))for(let V of Object.getOwnPropertyNames(k))V in j||Object.defineProperty(j,V,{get:()=>self[V]});(()=>{const k=()=>{const h=new Error("not implemented");return h.code="ENOSYS",h};if(!j.fs){let h="";j.fs={constants:{O_WRONLY:-1,O_RDWR:-1,O_CREAT:-1,O_TRUNC:-1,O_APPEND:-1,O_EXCL:-1},writeSync(o,s){h+=E.decode(s);const f=h.lastIndexOf(`
`);return f!=-1&&(console.log(h.substring(0,f)),h=h.substring(f+1)),s.length},write(o,s,f,w,_,x){if(f!==0||w!==s.length||_!==null){x(k());return}const d=this.writeSync(o,s);x(null,d)},chmod(o,s,f){f(k())},chown(o,s,f,w){w(k())},close(o,s){s(k())},fchmod(o,s,f){f(k())},fchown(o,s,f,w){w(k())},fstat(o,s){s(k())},fsync(o,s){s(null)},ftruncate(o,s,f){f(k())},lchown(o,s,f,w){w(k())},link(o,s,f){f(k())},lstat(o,s){s(k())},mkdir(o,s,f){f(k())},open(o,s,f,w){w(k())},read(o,s,f,w,_,x){x(k())},readdir(o,s){s(k())},readlink(o,s){s(k())},rename(o,s,f){f(k())},rmdir(o,s){s(k())},stat(o,s){s(k())},symlink(o,s,f){f(k())},truncate(o,s,f){f(k())},unlink(o,s){s(k())},utimes(o,s,f,w){w(k())}}}if(j.process||(j.process={getuid(){return-1},getgid(){return-1},geteuid(){return-1},getegid(){return-1},getgroups(){throw k()},pid:-1,ppid:-1,umask(){throw k()},cwd(){throw k()},chdir(){throw k()}}),!j.crypto)throw new Error("globalThis.crypto is not available, polyfill required (crypto.getRandomValues only)");if(!j.performance)throw new Error("globalThis.performance is not available, polyfill required (performance.now only)");if(!j.TextEncoder)throw new Error("globalThis.TextEncoder is not available, polyfill required");if(!j.TextDecoder)throw new Error("globalThis.TextDecoder is not available, polyfill required");const V=new TextEncoder("utf-8"),E=new TextDecoder("utf-8");j.Go=class{constructor(){this.argv=["js"],this.env={},this.exit=e=>{e!==0&&console.warn("exit code:",e)},this._exitPromise=new Promise(e=>{this._resolveExitPromise=e}),this._pendingEvent=null,this._scheduledTimeouts=new Map,this._nextCallbackTimeoutID=1;const h=(e,l)=>{this.mem.setUint32(e+0,l,!0),this.mem.setUint32(e+4,Math.floor(l/4294967296),!0)},o=e=>{const l=this.mem.getUint32(e+0,!0),p=this.mem.getInt32(e+4,!0);return l+p*4294967296},s=e=>{const l=this.mem.getFloat64(e,!0);if(l===0)return;if(!isNaN(l))return l;const p=this.mem.getUint32(e,!0);return this._values[p]},f=(e,l)=>{if(typeof l=="number"&&l!==0){if(isNaN(l)){this.mem.setUint32(e+4,2146959360,!0),this.mem.setUint32(e,0,!0);return}this.mem.setFloat64(e,l,!0);return}if(l===void 0){this.mem.setFloat64(e,0,!0);return}let y=this._ids.get(l);y===void 0&&(y=this._idPool.pop(),y===void 0&&(y=this._values.length),this._values[y]=l,this._goRefCounts[y]=0,this._ids.set(l,y)),this._goRefCounts[y]++;let S=0;switch(typeof l){case"object":l!==null&&(S=1);break;case"string":S=2;break;case"symbol":S=3;break;case"function":S=4;break}this.mem.setUint32(e+4,2146959360|S,!0),this.mem.setUint32(e,y,!0)},w=e=>{const l=o(e+0),p=o(e+8);return new Uint8Array(this._inst.exports.mem.buffer,l,p)},_=e=>{const l=o(e+0),p=o(e+8),y=new Array(p);for(let S=0;S<p;S++)y[S]=s(l+S*8);return y},x=e=>{const l=o(e+0),p=o(e+8);return E.decode(new DataView(this._inst.exports.mem.buffer,l,p))},d=Date.now()-performance.now();this.importObject={_gotest:{add:(e,l)=>e+l},gojs:{"runtime.wasmExit":e=>{e>>>=0;const l=this.mem.getInt32(e+8,!0);this.exited=!0,delete this._inst,delete this._values,delete this._goRefCounts,delete this._ids,delete this._idPool,this.exit(l)},"runtime.wasmWrite":e=>{e>>>=0;const l=o(e+8),p=o(e+16),y=this.mem.getInt32(e+24,!0);j.fs.writeSync(l,new Uint8Array(this._inst.exports.mem.buffer,p,y))},"runtime.resetMemoryDataView":e=>{this.mem=new DataView(this._inst.exports.mem.buffer)},"runtime.nanotime1":e=>{e>>>=0,h(e+8,(d+performance.now())*1e6)},"runtime.walltime":e=>{e>>>=0;const l=new Date().getTime();h(e+8,l/1e3),this.mem.setInt32(e+16,l%1e3*1e6,!0)},"runtime.scheduleTimeoutEvent":e=>{e>>>=0;const l=this._nextCallbackTimeoutID;this._nextCallbackTimeoutID++,this._scheduledTimeouts.set(l,setTimeout(()=>{for(this._resume();this._scheduledTimeouts.has(l);)console.warn("scheduleTimeoutEvent: missed timeout event"),this._resume()},o(e+8))),this.mem.setInt32(e+16,l,!0)},"runtime.clearTimeoutEvent":e=>{e>>>=0;const l=this.mem.getInt32(e+8,!0);clearTimeout(this._scheduledTimeouts.get(l)),this._scheduledTimeouts.delete(l)},"runtime.getRandomData":e=>{e>>>=0,crypto.getRandomValues(w(e+8))},"syscall/js.finalizeRef":e=>{e>>>=0;const l=this.mem.getUint32(e+8,!0);if(this._goRefCounts[l]--,this._goRefCounts[l]===0){const p=this._values[l];this._values[l]=null,this._ids.delete(p),this._idPool.push(l)}},"syscall/js.stringVal":e=>{e>>>=0,f(e+24,x(e+8))},"syscall/js.valueGet":e=>{e>>>=0;const l=Reflect.get(s(e+8),x(e+16));e=this._inst.exports.getsp()>>>0,f(e+32,l)},"syscall/js.valueSet":e=>{e>>>=0,Reflect.set(s(e+8),x(e+16),s(e+32))},"syscall/js.valueDelete":e=>{e>>>=0,Reflect.deleteProperty(s(e+8),x(e+16))},"syscall/js.valueIndex":e=>{e>>>=0,f(e+24,Reflect.get(s(e+8),o(e+16)))},"syscall/js.valueSetIndex":e=>{e>>>=0,Reflect.set(s(e+8),o(e+16),s(e+24))},"syscall/js.valueCall":e=>{e>>>=0;try{const l=s(e+8),p=Reflect.get(l,x(e+16)),y=_(e+32),S=Reflect.apply(p,l,y);e=this._inst.exports.getsp()>>>0,f(e+56,S),this.mem.setUint8(e+64,1)}catch(l){e=this._inst.exports.getsp()>>>0,f(e+56,l),this.mem.setUint8(e+64,0)}},"syscall/js.valueInvoke":e=>{e>>>=0;try{const l=s(e+8),p=_(e+16),y=Reflect.apply(l,void 0,p);e=this._inst.exports.getsp()>>>0,f(e+40,y),this.mem.setUint8(e+48,1)}catch(l){e=this._inst.exports.getsp()>>>0,f(e+40,l),this.mem.setUint8(e+48,0)}},"syscall/js.valueNew":e=>{e>>>=0;try{const l=s(e+8),p=_(e+16),y=Reflect.construct(l,p);e=this._inst.exports.getsp()>>>0,f(e+40,y),this.mem.setUint8(e+48,1)}catch(l){e=this._inst.exports.getsp()>>>0,f(e+40,l),this.mem.setUint8(e+48,0)}},"syscall/js.valueLength":e=>{e>>>=0,h(e+16,parseInt(s(e+8).length))},"syscall/js.valuePrepareString":e=>{e>>>=0;const l=V.encode(String(s(e+8)));f(e+16,l),h(e+24,l.length)},"syscall/js.valueLoadString":e=>{e>>>=0;const l=s(e+8);w(e+16).set(l)},"syscall/js.valueInstanceOf":e=>{e>>>=0,this.mem.setUint8(e+24,s(e+8)instanceof s(e+16)?1:0)},"syscall/js.copyBytesToGo":e=>{e>>>=0;const l=w(e+8),p=s(e+32);if(!(p instanceof Uint8Array||p instanceof Uint8ClampedArray)){this.mem.setUint8(e+48,0);return}const y=p.subarray(0,l.length);l.set(y),h(e+40,y.length),this.mem.setUint8(e+48,1)},"syscall/js.copyBytesToJS":e=>{e>>>=0;const l=s(e+8),p=w(e+16);if(!(l instanceof Uint8Array||l instanceof Uint8ClampedArray)){this.mem.setUint8(e+48,0);return}const y=p.subarray(0,l.length);l.set(y),h(e+40,y.length),this.mem.setUint8(e+48,1)},debug:e=>{console.log(e)}}}}async run(h){if(!(h instanceof WebAssembly.Instance))throw new Error("Go.run: WebAssembly.Instance expected");this._inst=h,this.mem=new DataView(this._inst.exports.mem.buffer),this._values=[NaN,0,null,!0,!1,j,this],this._goRefCounts=new Array(this._values.length).fill(1/0),this._ids=new Map([[0,1],[null,2],[!0,3],[!1,4],[j,5],[this,6]]),this._idPool=[],this.exited=!1;let o=4096;const s=e=>{const l=o,p=V.encode(e+"\0");return new Uint8Array(this.mem.buffer,o,p.length).set(p),o+=p.length,o%8!==0&&(o+=8-o%8),l},f=this.argv.length,w=[];this.argv.forEach(e=>{w.push(s(e))}),w.push(0),Object.keys(this.env).sort().forEach(e=>{w.push(s(`${e}=${this.env[e]}`))}),w.push(0);const x=o;if(w.forEach(e=>{this.mem.setUint32(o,e,!0),this.mem.setUint32(o+4,0,!0),o+=8}),o>=12288)throw new Error("total length of command line and environment variables exceeds limit");this._inst.exports.run(f,x),this.exited&&this._resolveExitPromise(),await this._exitPromise}_resume(){if(this.exited)throw new Error("Go program has already exited");this._inst.exports.resume(),this.exited&&this._resolveExitPromise()}_makeFuncWrapper(h){const o=this;return function(){const s={id:h,this:this,args:arguments};return o._pendingEvent=s,o._resume(),s.result}}}})(),M=({data:k})=>{let V=new TextDecoder,E=j.fs,h="";E.writeSync=(_,x)=>{if(_===1)D(x);else if(_===2){h+=V.decode(x);let d=h.split(`
`);d.length>1&&console.log(d.slice(0,-1).join(`
`)),h=d[d.length-1]}else throw new Error("Bad write");return x.length};let o=[],s,f=0;M=({data:_})=>(_.length>0&&(o.push(_),s&&s()),w),E.read=(_,x,d,e,l,p)=>{if(_!==0||d!==0||e!==x.length||l!==null)throw new Error("Bad read");if(o.length===0){s=()=>E.read(_,x,d,e,l,p);return}let y=o[0],S=Math.max(0,Math.min(e,y.length-f));x.set(y.subarray(f,f+S),d),f+=S,f===y.length&&(o.shift(),f=0),p(null,S)};let w=new j.Go;return w.argv=["","--service=0.24.2"],C(k,w).then(_=>{D(null),w.run(_)},_=>{D(_)}),w};async function C(k,V){if(k instanceof WebAssembly.Module)return WebAssembly.instantiate(k,V.importObject);const E=await fetch(k);if(!E.ok)throw new Error(`Failed to download ${JSON.stringify(k)}`);if("instantiateStreaming"in WebAssembly&&/^application\/wasm($|;)/i.test(E.headers.get("Content-Type")||""))return(await WebAssembly.instantiateStreaming(E,V.importObject)).instance;const h=await E.arrayBuffer();return(await WebAssembly.instantiate(h,V.importObject)).instance}return k=>M(k)})(D=>a.onmessage({data:D})),L;a={onmessage:null,postMessage:D=>setTimeout(()=>{try{L=$({data:D})}catch(M){m(M)}}),terminate(){if(L)for(let D of L._scheduledTimeouts.values())clearTimeout(D)}}}let c,v;const u=new Promise(($,L)=>{c=$,v=L});a.onmessage=({data:$})=>{a.onmessage=({data:L})=>I(L),$?v($):c()},a.postMessage(n||new URL(t,location.href).toString());let{readFromStdout:I,service:U}=nt({writeToStdin($){a.postMessage($)},isSync:!1,hasFS:!1,esbuild:xe});await u,pe=()=>{a.terminate(),re=void 0,pe=void 0,we=void 0},we={build:$=>new Promise((L,D)=>{g.then(D),U.buildOrContext({callName:"build",refs:null,options:$,isTTY:!1,defaultWD:"/",callback:(M,j)=>M?D(M):L(j)})}),context:$=>new Promise((L,D)=>{g.then(D),U.buildOrContext({callName:"context",refs:null,options:$,isTTY:!1,defaultWD:"/",callback:(M,j)=>M?D(M):L(j)})}),transform:($,L)=>new Promise((D,M)=>{g.then(M),U.transform({callName:"transform",refs:null,input:$,options:L||{},isTTY:!1,fs:{readFile(j,C){C(new Error("Internal error"),null)},writeFile(j,C){C(null)}},callback:(j,C)=>j?M(j):D(C)})}),formatMessages:($,L)=>new Promise((D,M)=>{g.then(M),U.formatMessages({callName:"formatMessages",refs:null,messages:$,options:L,callback:(j,C)=>j?M(j):D(C)})}),analyzeMetafile:($,L)=>new Promise((D,M)=>{g.then(M),U.analyzeMetafile({callName:"analyzeMetafile",refs:null,metafile:typeof $=="string"?$:JSON.stringify($),options:L,callback:(j,C)=>j?M(j):D(C)})})}},yt=xe,bt="/framekit-js/assets/esbuild-DcsFl_sZ.wasm";const Ve=import.meta.url,vt=new URL("../datasets/",Ve).href;let je=!1;async function xt(){je||(await Le({wasmURL:bt,worker:!1}),je=!0)}let Pe=!1;async function kt(){if(Pe)return;const t=await import("./browser-B9MCPMmA.js");Object.assign(self,t,{__DATASETS_BASE__:vt}),Pe=!0}function Et(t){return new Promise(n=>{const r=[],a={log:console.log,warn:console.warn,error:console.error,info:console.info},m=c=>(...v)=>r.push({timestamp:performance.now(),level:c,args:v});console.log=m("log"),console.warn=m("warn"),console.error=m("error"),console.info=m("info");const g=()=>Object.assign(console,a);t().then(c=>{g(),n({logs:r,result:c,error:null})}).catch(c=>{g(),n({logs:r,result:null,error:c})})})}function _t(t){return t!==null&&typeof t=="object"&&typeof t.toArray=="function"&&Array.isArray(t.columns)}function St(t){return t.columns.map(n=>({name:n,dtype:String(t.dtypes[n]??"unknown")}))}async function Tt(t){const r=`async function __run__() {
${t.replace(/^\s*import\s[^;]+;?\s*$/gm,"")}
}`,{code:a}=await Ne(r,{loader:"ts",format:"esm",target:"es2022",define:{"import.meta.url":JSON.stringify(Ve)}}),m=await new Function(`${a}
return __run__();`)();return _t(m)?{rows:m.toArray(),schema:St(m)}:{rows:[],schema:[]}}self.onmessage=async t=>{const n=t.data;if(n.type!=="run")return;const r=performance.now();try{await xt(),await kt()}catch(I){self.postMessage({type:"error",error:`Init failed: ${String(I)}`,logs:[],executionTime:0});return}const{logs:a,result:m,error:g}=await Et(()=>Tt(n.code)),c=performance.now()-r;if(g!==null){self.postMessage({type:"error",error:g instanceof Error?`${g.message}
${g.stack??""}`:String(g),logs:a,executionTime:c});return}const{rows:v,schema:u}=m;self.postMessage({type:"success",rows:v,schema:u,logs:a,executionTime:c})};
//# sourceMappingURL=executor.worker-DgWsm8MH.js.map
