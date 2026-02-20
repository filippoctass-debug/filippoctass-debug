
document.addEventListener("DOMContentLoaded", () => {
  try{
    document.title = "Control Room – LOADED";
    try{ const hl=document.getElementById("hostLabel"); if(hl) hl.innerText = (location.host || "—"); }catch(e){}
    try{ const as=document.getElementById("apiStatus"); if(as) as.innerText = "API: …"; }catch(e){}
    const m = document.createElement("div");
    m.id="__boot_marker";
    m.textContent="BOOT OK " + new Date().toISOString();
    m.style.position="fixed";
    m.style.left="10px";
    m.style.bottom="10px";
    m.style.zIndex="99999";
    m.style.padding="6px 10px";
    m.style.borderRadius="10px";
    m.style.background="rgba(0,0,0,.6)";
    m.style.border="1px solid rgba(255,255,255,.2)";
    m.style.color="white";
    m.style.fontFamily="ui-monospace,Consolas,monospace";
    m.style.fontSize="12px";
    document.body.appendChild(m);
    console.log("BOOT_MARKER_OK");
    try{
      let k=0;
      const t=setInterval(()=>{
        k++;
        try{
          const hl=document.getElementById("hostLabel");
          if(hl) hl.innerText = (location.host || "—");
          if(k>=5) clearInterval(t);
        }catch(e){}
      }, 1000);
    }catch(e){}}catch(e){}
  const $ = (id) => document.getElementById(id);

// --- EARLY BOOT (must not crash) ---
try { $("hostLabel").innerText = (location.host || "—"); } catch(e) {}
try {
  fetch("/openapi.json", { cache:"no-store" })
    .then(r => { try{$("apiStatus").innerText = `API: ${r.status}`;}catch(e){} })
    .catch(_ => { try{$("apiStatus").innerText = "API: err";}catch(e){} });
} catch(e) {}

  let selected = null;
  let selectedDevice = null; // {site_id, id, name}
  let expandedSites = {};
  let sitesStatus = [];   // from WS
  let allSites = [];      // from /sites
  let devicesBySite = {}; // siteId -> devices[]
  let deviceStatusBySite = {}; // site_id -> status devices
  let rangeMinutes = Number(localStorage.getItem("rangeMinutes") || 120);
  if(!isFinite(rangeMinutes) || rangeMinutes<=0) rangeMinutes = 120;

  let ws = null;

  // Charts
  let chartPV = null;
let chartInv = null;
let chartWeather = null;

function safeInitCharts(){
  if(!window.echarts){
    try{ pushEvent("Charts", "ECharts non caricato (CDN bloccato/offline). Dashboard avviata senza grafici.", "warn"); }catch(e){}
    return;
  }
  try{
    chartPV = echarts.init($("chartPV"));
    chartInv = echarts.init($("chartInv"));
    chartWeather = echarts.init($("chartWeather"));
  }catch(e){
    try{ pushEvent("Charts", "Errore init grafici: " + (e.message||e), "err"); }catch(_){}
  }
}
safeInitCharts();

  function fmt(v, unit="") {
    if (v === null || v === undefined) return "—";
    if (typeof v !== "number") return String(v);
    if (unit === "W") {
      if (Math.abs(v) >= 1000) return (v/1000).toFixed(2) + " kW";
      return v.toFixed(0) + " W";
    }
    if (unit === "V") return v.toFixed(0) + " V";
    if (unit === "Hz") return v.toFixed(2) + " Hz";
    if (unit === "°C") return v.toFixed(1) + " °C";
    if (unit === "%") return v.toFixed(0) + " %";
    return v.toFixed(2) + (unit ? (" "+unit) : "");
  }

  function pushEvent(title, desc, kind="info"){
    const el = $("events");
    const node = document.createElement("div");
    node.className = "evt";
    const stamp = new Date().toLocaleTimeString();
    const k = (kind === "warn") ? "warn" : (kind === "err" ? "err" : "info");
    node.innerHTML = `
      <div class="t">${title} <span style="color:var(--muted);font-weight:600;font-size:12px;">(${stamp})</span></div>
      <div class="d">${desc}</div>
      <div class="k">${k}</div>
    `;
    el.prepend(node);
    while(el.children.length > 10) el.removeChild(el.lastChild);
  }

  function setNoDataPV(show, msg="Nessun dato nel range selezionato"){
    let el = document.getElementById("chartNoData");
    if(!el){
      el = document.createElement("div");
      el.id = "chartNoData";
      el.style.position="absolute";
      el.style.inset="0";
      el.style.display="flex";
      el.style.alignItems="center";
      el.style.justifyContent="center";
      el.style.pointerEvents="none";
      el.style.color="var(--muted)";
      el.style.fontWeight="700";
      el.style.letterSpacing=".2px";
      el.style.fontSize="13px";
      const host = $("chartPV").parentElement;
      if(host) host.style.position="relative";
      (host || document.body).appendChild(el);
    }
    el.textContent = msg;
    el.style.opacity = show ? "1" : "0";
  }

  if(chartPV) chartPV.setOption({
    grid: { left: 46, right: 16, top: 26, bottom: 34 },
    tooltip: { trigger: 'axis' },
    xAxis: { type:'category', boundaryGap:false, axisLabel:{ color:'#9fb0d0'} },
    yAxis: [
      { type:'value', name:'W', axisLabel:{ color:'#9fb0d0'} },
      { type:'value', name:'W/m²', axisLabel:{ color:'#9fb0d0'} }
    ],
    series: [
      { name:'Potenza AC (W)', type:'line', smooth:true, showSymbol:false, data:[] },
      { name:'POA (W/m²)', type:'line', smooth:true, showSymbol:false, yAxisIndex:1, data:[] }
    ]
  });

  function showPVView(){
    $("mainPanelTitle").innerText = "Irraggiamento vs Potenza";
    $("mainPanelSub").innerText = "Aggiornamento live (ultimi minuti)";
    $("panelInv").style.display = "none";
    $("panelWeather").style.display = "none";
    $("chartPV").style.display = "";
    if(chartPV) chartPV.resize();
  }

  function showInvView(){
    $("mainPanelTitle").innerText = "Dettaglio inverter";
    $("mainPanelSub").innerText = "Valori live + trend";
    $("chartPV").style.display = "none";
    $("panelWeather").style.display = "none";
    $("panelInv").style.display = "";
    if(chartInv) chartInv.resize();
  }

  function showWeatherPanel(){
    $("panelWeather").style.display = "";
    if(chartWeather) chartWeather.resize();
  }

  async function fetchSites(){
    try{
      // ping API (non serve una lista /sites, usiamo WS per i PV online)
      const r = await fetch("/openapi.json", { cache: "no-store" });
      $("apiStatus").innerText = `API: ${r.status}`;
    }catch(e){
      $("apiStatus").innerText = `API: err`;
    }

    // Lista PV ricavata dallo stream WS (e persistita)
    allSites = (sitesStatus || []).map(s => ({
      site_id: String(s.site_id),
      online: (("ok" in s) ? !!s.ok : true),
      last_edge_status: s.last_ts || null
    }));

    // fallback: se WS non ha ancora dati, prova da localStorage
    if(!allSites.length){
      try{
        const raw = localStorage.getItem("knownSites") || "[]";
        const arr = JSON.parse(raw);
        if(Array.isArray(arr)){
          allSites = arr.map(x => ({ site_id: String(x.site_id), online: !!x.online, last_edge_status: x.last_edge_status || null }));
        }
      }catch(e){}
    }

    // persisti
    try{
      localStorage.setItem("knownSites", JSON.stringify(allSites.slice(0,200)));
    }catch(e){}

    if(!selected && allSites.length) setSelected(allSites[0].site_id);
    renderSites();
}

  async function fetchDevices(siteId){
    try{
      const r = await fetch(`/api/site/${encodeURIComponent(siteId)}/devices`, { cache:"no-store" });
      if(!r.ok) throw new Error(`HTTP ${r.status}`);
      const j = await r.json();
      devicesBySite[String(siteId)] = (j.devices || []);
    }catch(e){
      devicesBySite[String(siteId)] = [];
      pushEvent("Devices", `Impossibile caricare devices per ${siteId}: ${e.message}`, "warn");
    }
  }

  async function fetchDeviceStatus(siteId){
    try{
      const r = await fetch(`/api/site/${encodeURIComponent(siteId)}/devices/status`, { cache:"no-store" });
      if(!r.ok) throw new Error(`HTTP ${r.status}`);
      const j = await r.json();
      deviceStatusBySite[String(siteId)] = j;
    }catch(e){
      deviceStatusBySite[String(siteId)] = { site_id:String(siteId), total:0, online:0, devices:[] };
    }
  }

  function getDeviceOk(siteId, deviceId){
    const ds = deviceStatusBySite[String(siteId)];
    if(ds && Array.isArray(ds.devices)){
      const d = ds.devices.find(x => String(x.id||"") === String(deviceId));
      if(d && typeof d.ok === "boolean") return d.ok;
    }
    return null;
  }

  function renderSites(){
    const q = $("q").value.trim().toLowerCase();
    const el = $("siteList");
    el.innerHTML = "";

    const list = (allSites || [])
      .map(s => ({...s, site_id: String(s.site_id)}))
      .filter(s => s.site_id.toLowerCase().includes(q));

    if(list.length === 0){
      el.innerHTML = `<div style="color:#9fb0d0;font-size:12px;padding:10px;line-height:1.35;">
        Nessun impianto ancora.<br>
        Se l’edge è online ma non compare, significa che non sta scrivendo punti su Influx (o tag/measurement non compatibili).
      </div>`;
      return;
    }

    for(const s of list){
      const sid = String(s.site_id);
      const st = (sitesStatus || []).find(x => String(x.site_id) === sid);

      const pvOk = (st && ("ok" in st)) ? !!st.ok : !!s.online;

      const age = (st && (st.age_s != null)) ? st.age_s : (() => {
        try{
          if(!s.last_edge_status) return null;
          const t = new Date(s.last_edge_status).getTime();
          if(!isFinite(t)) return null;
          return Math.max(0, Math.round((Date.now()-t)/1000));
        }catch(e){ return null; }
      })();

      const activePV = (sid === String(selected)) && !selectedDevice;

      const row = document.createElement("div");
      row.className = "site" + (activePV ? " active" : "");
      row.onclick = () => {
        selectedDevice = null;
        setSelected(sid);
        showPVView();
      };

      const expanded = !!expandedSites[sid];
      row.innerHTML = `
        <div class="siteRow">
          <div class="left">
            <div class="dot ${pvOk ? "ok" : "bad"}"></div>
            <div>
              <div class="name">${sid}</div>
              <div class="meta">${(age!=null) ? ("age " + age + "s") : (pvOk ? "online (da /sites)" : "offline (da /sites)")}</div>
            </div>
          </div>
          <div class="siteRight">
            <button class="toggleBtn" data-kind="toggle" title="Espandi">${expanded ? "▾" : "▸"}</button>
            <div class="badge">${pvOk ? "ONLINE" : "OFFLINE"}${(() => { const ds=deviceStatusBySite[sid]; if(!ds) return ""; return " | " + ds.online + "/" + ds.total; })()}</div>
          </div>
        </div>
      `;

      const tgl = row.querySelector('[data-kind="toggle"]');
      if(tgl){
        tgl.onclick = async (ev) => {
          ev.stopPropagation();
          await fetchDeviceStatus(sid);
          expandedSites[sid] = !expandedSites[sid];
          if(expandedSites[sid] && !devicesBySite[sid]) await fetchDevices(sid);
          renderSites();
        };
      }

      el.appendChild(row);

      if(expandedSites[sid]){
        const devs = (devicesBySite[sid] || []);
        for(const d of devs){
          const did = String(d.id || d.name || ("dev"+(devs.indexOf(d)+1)));
          const nm = d.name || did || "device";
          const invOk = getDeviceOk(sid, did);

          const child = document.createElement("div");
          child.className = "site dev";
          const activeDev = selectedDevice && selectedDevice.site_id===sid && selectedDevice.id===did;
          if(activeDev) child.classList.add("active");

          child.onclick = () => {
            selected = sid;
            selectedDevice = { site_id: sid, id: did, name: nm };
            renderSites();
            showInvView();
            renderInverter(sid, did, nm);
          };

          const dotCls = (invOk === true) ? "ok" : (invOk === false ? "bad" : "unk");

          child.innerHTML = `
            <div class="siteRow">
              <div class="left">
                <div class="dot ${dotCls}"></div>
                <div>
                  <div class="name">${nm}</div>
                  <div class="meta">${d.host || ""}:${d.port || ""} unit ${d.unit_id || ""}</div>
                </div>
              </div>
              <div class="siteRight">
                <div class="badge">INV</div>
              </div>
            </div>
          `;
          el.appendChild(child);
        }
      }
    }
  }

  function refreshSelectedKpi(){
    if(!selected){
      $("k_status").innerText="—";
      $("k_age").innerText="—";
      $("k_pac").innerText="—";
      $("k_weather").innerText="—";
      $("k_weather_hint").innerText="Temp / Cloud / UV";
      $("k_health").innerText="—";
      $("k_status").style.color = "var(--text)";
      $("k_health").style.color = "var(--muted)";
      return;
    }

    const st = (sitesStatus || []).find(x => String(x.site_id) === String(selected));
    const ss = (allSites || []).find(x => String(x.site_id) === String(selected));

    const ok = (st && ("ok" in st)) ? !!st.ok : !!(ss && ss.online);

    const age = (st && (st.age_s != null)) ? st.age_s : (() => {
      try{
        if(!ss || !ss.last_edge_status) return null;
        const t = new Date(ss.last_edge_status).getTime();
        if(!isFinite(t)) return null;
        return Math.max(0, Math.round((Date.now()-t)/1000));
      }catch(e){ return null; }
    })();

    $("k_status").innerText = ok ? "ONLINE" : "OFFLINE";
    $("k_status").style.color = ok ? "var(--ok)" : "var(--bad)";
    const last = (st && st.last_ts) ? st.last_ts : ((ss && ss.last_edge_status) ? ss.last_edge_status : "?");
    $("k_age").innerText = (age!=null) ? (`Ultimo dato: ${last} | age ${age}s`) : "Nessun dato ancora";
    $("k_health").innerText = ok ? "OK" : ((age!=null) ? "STALE" : "—");
    $("k_health").style.color = ok ? "var(--ok)" : ((age!=null) ? "var(--warn)" : "var(--muted)");
  }

  async function fetchSeriesPV(){
    if(!selected) return;
    try{
      const minutes = rangeMinutes;
      $("rangePill").innerText = `range: ${minutes}m`;
      const r = await fetch(`/api/site/${encodeURIComponent(selected)}/series?minutes=${minutes}&every=10s`, { cache:"no-store" });
      if(!r.ok) throw new Error(`HTTP ${r.status}`);
      const j = await r.json();

      const t = (j.t || []).map(x => new Date(x).toLocaleTimeString());
      const pac = Array.isArray(j.p_ac_w) ? j.p_ac_w : [];
      const poa = Array.isArray(j.poa_wm2) ? j.poa_wm2 : [];

      if(pac.length) $("k_pac").innerText = fmt(pac[pac.length-1], "W");

      if(!t.length){
        setNoDataPV(true);
        return;
      }
      setNoDataPV(false);

      if(chartPV) chartPV.setOption({
        xAxis: { data: t },
        series: [
          { name: 'Potenza AC (W)', type: 'line', smooth:true, showSymbol:false, data: pac },
          { name: 'POA (W/m²)',     type: 'line', smooth:true, showSymbol:false, yAxisIndex: 1, data: poa }
        ]
      }, { notMerge:false, lazyUpdate:true });

    }catch(e){
      pushEvent("Serie", `Impossibile caricare serie PV: ${e.message}`, "warn");
    }
  }

  let invFailUntil = 0;
  async function renderInverter(siteId, deviceId, deviceName){
    if(Date.now() < invFailUntil) return;

    $("title").innerText = `${deviceName || deviceId}`;
    $("subtitle").innerText = `Dettaglio inverter ${deviceId} (${siteId})`;

    // latest
    try{
      const r = await fetch(`/api/site/${encodeURIComponent(siteId)}/device/${encodeURIComponent(deviceId)}/latest`, { cache:"no-store" });
      if(!r.ok){
        if(r.status===404) invFailUntil = Date.now()+10000;
        throw new Error(`HTTP ${r.status}`);
      }
      const j = await r.json();

      const f = (j.fields || {});
      const pac = f.p_ac_w ?? null;
      const vdc = f.v_dc_v ?? null;
      const gridv = f.grid_v ?? null;
      const hz = f.freq_hz ?? null;

      $("inv_pac").innerText = fmt((typeof pac==='number')?pac:null, "W");
      $("inv_vdc").innerText = fmt((typeof vdc==='number')?vdc:null, "V");
      $("inv_gridv").innerText = fmt((typeof gridv==='number')?gridv:null, "V");
      $("inv_hz").innerText = fmt((typeof hz==='number')?hz:null, "Hz");
      $("inv_ts").innerText = j.last_ts ? ("ts " + j.last_ts) : "—";
      $("inv_raw").textContent = JSON.stringify(j, null, 2);

    }catch(e){
      pushEvent("INV", `latest: ${e.message}`, "warn");
      $("inv_raw").textContent = "(errore latest: " + e.message + ")";
    }

    // series
    try{
      const minutes = rangeMinutes;
      const r2 = await fetch(`/api/site/${encodeURIComponent(siteId)}/device/${encodeURIComponent(deviceId)}/series?minutes=${minutes}&every=10s`, { cache:"no-store" });
      if(!r2.ok){
        if(r2.status===404) invFailUntil = Date.now()+10000;
        throw new Error(`HTTP ${r2.status}`);
      }
      const j2 = await r2.json();
      const t = (j2.t || []).map(x => new Date(x).toLocaleTimeString());
      const pac = Array.isArray(j2.p_ac_w) ? j2.p_ac_w : [];

      if(chartInv) chartInv.setOption({
        grid: { left: 46, right: 16, top: 26, bottom: 34 },
        tooltip: { trigger:'axis' },
        xAxis: { type:'category', boundaryGap:false, data: t, axisLabel:{ color:'#9fb0d0'} },
        yAxis: { type:'value', axisLabel:{ color:'#9fb0d0'} },
        series: [{ name:'P_AC (W)', type:'line', smooth:true, showSymbol:false, data: pac }]
      }, { notMerge:true, lazyUpdate:true });

    }catch(e){
      pushEvent("INV", `series: ${e.message}`, "warn");
    }
  }

  async function loadWeatherForSelected(){
    if(!selected) return;
    try{
      const r = await fetch(`/api/site/${encodeURIComponent(selected)}/weather`, { cache:"no-store" });
      if(!r.ok){
        // se non configurato -> 400 (lat/lon missing)
        $("k_weather").innerText = "—";
        $("k_weather_hint").innerText = (r.status===400) ? "Config meteo non impostata" : ("Meteo HTTP " + r.status);
        return;
      }
      const j = await r.json();
      const c = j.current || {};
      const t = (typeof c.temperature_2m === "number") ? c.temperature_2m : null;
      const cl = (typeof c.cloud_cover === "number") ? c.cloud_cover : null;
      const uv = (typeof c.uv_index === "number") ? c.uv_index : null;

      $("k_weather").innerText = (t==null && cl==null && uv==null) ? "—" : `${fmt(t,"°C")} | ${fmt(cl,"%")} | UV ${uv==null?"—":uv.toFixed(2)}`;
      $("k_weather_hint").innerText = "Temp | Cloud | UV";

      // forecast chart
      const ht = (j.production_est_kw || {}).time || [];
      const pk = (j.production_est_kw || {}).p_kw || [];
      const hourly = j.hourly || {};
      const cloud = hourly.cloud_cover || [];
      const uu = hourly.uv_index || [];

      const x = ht.map(s => {
        try{
          const d = new Date(s);
          return d.toLocaleString(undefined, {month:'2-digit', day:'2-digit', hour:'2-digit', minute:'2-digit'});
        }catch(e){ return s; }
      });

      if(chartWeather) chartWeather.setOption({
        grid: { left: 46, right: 16, top: 26, bottom: 34 },
        tooltip: { trigger:'axis' },
        xAxis: { type:'category', boundaryGap:false, data: x, axisLabel:{ color:'#9fb0d0', hideOverlap:true } },
        yAxis: [
          { type:'value', name:'kW', axisLabel:{ color:'#9fb0d0'} },
          { type:'value', name:'%', axisLabel:{ color:'#9fb0d0'} }
        ],
        series: [
          { name:'Prod. stimata (kW)', type:'line', smooth:true, showSymbol:false, data: pk },
          { name:'Cloud (%)', type:'line', smooth:true, showSymbol:false, yAxisIndex:1, data: cloud },
          { name:'UV', type:'line', smooth:true, showSymbol:false, data: uu }
        ]
      }, { notMerge:true, lazyUpdate:true });

    }catch(e){
      $("k_weather").innerText = "—";
      $("k_weather_hint").innerText = "Meteo non disponibile";
    }
  }

  function setSelected(siteId){
    selected = siteId;
    selectedDevice = null;

    $("title").innerText = `Impianto ${siteId}`;
    $("subtitle").innerText = `Telemetria & KPI live per ${siteId}.`;

    refreshSelectedKpi();
    showPVView();
    fetchSeriesPV();
    loadWeatherForSelected();
    renderSites();
  }

  // ----- Drawer -----
  function openDrawer(){ $("drawerOverlay").classList.add("show"); $("drawerOverlay").setAttribute("aria-hidden","false"); }
  function closeDrawer(){ $("drawerOverlay").classList.remove("show"); $("drawerOverlay").setAttribute("aria-hidden","true"); }

  $("btnSettings").addEventListener("click", openDrawer);
  $("btnCloseDrawer").addEventListener("click", closeDrawer);
  $("btnCloseDrawer2").addEventListener("click", closeDrawer);
  $("drawerOverlay").addEventListener("click", (e) => { if(e.target === $("drawerOverlay")) closeDrawer(); });

  // ----- Modbus table helpers -----
  function setBoxMsg(id, text, kind="info"){
    const el = $(id);
    el.className = "msg" + (kind==="err" ? " err" : kind==="ok" ? " ok" : "");
    el.textContent = text;
  }

  function mbRowTemplate(d){
    const tr = document.createElement("tr");

    const tdEnabled = document.createElement("td");
    const chk = document.createElement("input");
    chk.type = "checkbox"; chk.checked = !!d.enabled;
    tdEnabled.appendChild(chk);

    const tdName = document.createElement("td");
    const inName = document.createElement("input");
    inName.value = d.name || "";
    tdName.appendChild(inName);

    const tdHost = document.createElement("td");
    const inHost = document.createElement("input");
    inHost.value = d.host || "";
    tdHost.appendChild(inHost);

    const tdPort = document.createElement("td");
    const inPort = document.createElement("input");
    inPort.type = "number"; inPort.value = (d.port ?? 502);
    tdPort.appendChild(inPort);

    const tdUnit = document.createElement("td");
    const inUnit = document.createElement("input");
    inUnit.type = "number"; inUnit.value = (d.unit_id ?? 1);
    tdUnit.appendChild(inUnit);

    const tdId = document.createElement("td");
    const inId = document.createElement("input");
    inId.value = d.id || ""; inId.placeholder = "(auto)";
    tdId.appendChild(inId);

    const tdActions = document.createElement("td");
    const del = document.createElement("button");
    del.className = "btn danger";
    del.style.padding = "8px 10px";
    del.textContent = "Remove";
    del.addEventListener("click", () => { tr.remove(); mbUpdateTargets(); });
    tdActions.appendChild(del);

    tr.appendChild(tdEnabled); tr.appendChild(tdName); tr.appendChild(tdHost);
    tr.appendChild(tdPort); tr.appendChild(tdUnit); tr.appendChild(tdId); tr.appendChild(tdActions);

    tr._getDevice = () => ({
      id: (inId.value.trim() || inName.value.trim() || "device"),
      name: inName.value.trim() || "device",
      host: inHost.value.trim(),
      port: Number(inPort.value || 502),
      unit_id: Number(inUnit.value || 1),
      enabled: chk.checked
    });

    [chk, inName, inHost, inPort, inUnit, inId].forEach(el => el.addEventListener("input", mbUpdateTargets));
    return tr;
  }

  function mbDevicesFromTable(){
    const rows = Array.from($("mbTbody").querySelectorAll("tr"));
    return rows.map(r => r._getDevice()).filter(d => d.host);
  }
  function mbUpdateTargets(){
    const devs = mbDevicesFromTable().filter(d => d.enabled);
    const parts = devs.map(d => `${d.host}:${d.port}:${d.unit_id}:${(d.name||'dev')}:${(d.id||'')}`.replace(/:$/,''));
    $("mbTargets").textContent = parts.join(",");
  }
  function mbAddEmpty(){
    $("mbTbody").appendChild(mbRowTemplate({ enabled:true, name:"device", host:"", port:502, unit_id:1, id:"" }));
    mbUpdateTargets();
  }

  async function mbLoad(siteId){
    const url = `/api/site/${encodeURIComponent(siteId)}/devices`;
    try{
      setBoxMsg("mbMsg", `Loading ${url} …`);
      const r = await fetch(url, { cache:"no-store" });
      if(r.status === 404){
        setBoxMsg("mbMsg", `Endpoint non disponibile: ${url} (404).`, "err");
        $("mbTbody").innerHTML = "";
        mbUpdateTargets();
        return;
      }
      if(!r.ok) throw new Error(`HTTP ${r.status}`);
      const cfg = await r.json();
      $("mbTbody").innerHTML = "";
      (cfg.devices || []).forEach(d => $("mbTbody").appendChild(mbRowTemplate(d)));
      mbUpdateTargets();
      setBoxMsg("mbMsg", "Loaded.", "ok");
      // refresh tree
      devicesBySite[String(siteId)] = (cfg.devices || []);
      renderSites();
    }catch(e){
      setBoxMsg("mbMsg", `Load error: ${e.message}`, "err");
    }
  }

  async function mbSave(siteId){
    const url = `/api/site/${encodeURIComponent(siteId)}/devices`;
    try{
      setBoxMsg("mbMsg", `Saving ${url} …`);
      const payload = { site_id: siteId, devices: mbDevicesFromTable() };
      const r = await fetch(url, {
        method:"PUT",
        headers:{ "Content-Type":"application/json" },
        body: JSON.stringify(payload)
      });
      if(r.status === 404){
        setBoxMsg("mbMsg", `Endpoint non disponibile: ${url} (404).`, "err");
        return;
      }
      if(!r.ok) throw new Error(`HTTP ${r.status}`);
      const cfg = await r.json();
      $("mbTbody").innerHTML = "";
      (cfg.devices || []).forEach(d => $("mbTbody").appendChild(mbRowTemplate(d)));
      mbUpdateTargets();
      setBoxMsg("mbMsg", "Saved.", "ok");
      devicesBySite[String(siteId)] = (cfg.devices || []);
      renderSites();
    }catch(e){
      setBoxMsg("mbMsg", `Save error: ${e.message}`, "err");
    }
  }

  // ----- Weather cfg -----
  async function wxLoad(siteId){
    const url = `/api/site/${encodeURIComponent(siteId)}/weather_cfg`;
    try{
      setBoxMsg("wxMsg", `Loading ${url} …`);
      const r = await fetch(url, { cache:"no-store" });
      if(!r.ok) throw new Error(`HTTP ${r.status}`);
      const j = await r.json();
      const c = j.cfg || {};
      $("wxLat").value = (c.lat ?? "");
      $("wxLon").value = (c.lon ?? "");
      $("wxKwp").value = (c.pv_kwp ?? "");
      $("wxTz").value = (c.timezone ?? "UTC");
      setBoxMsg("wxMsg", c.lat!=null ? "Loaded." : "No cfg set yet.", c.lat!=null ? "ok" : "info");
    }catch(e){
      setBoxMsg("wxMsg", `Load error: ${e.message}`, "err");
    }
  }

  async function wxSave(siteId){
    const lat = $("wxLat").value.trim();
    const lon = $("wxLon").value.trim();
    const kwp = $("wxKwp").value.trim() || "10";
    const tz = $("wxTz").value.trim() || "UTC";
    const url = `/api/site/${encodeURIComponent(siteId)}/weather_cfg?lat=${encodeURIComponent(lat)}&lon=${encodeURIComponent(lon)}&pv_kwp=${encodeURIComponent(kwp)}&timezone=${encodeURIComponent(tz)}&pr=0.85`;
    try{
      setBoxMsg("wxMsg", `Saving ${url} …`);
      const r = await fetch(url, { method:"PUT" });
      if(!r.ok) throw new Error(`HTTP ${r.status}`);
      setBoxMsg("wxMsg", "Saved.", "ok");
      loadWeatherForSelected();
    }catch(e){
      setBoxMsg("wxMsg", `Save error: ${e.message}`, "err");
    }
  }

  // ----- WS -----
  function connectWS(){
    try{
      const proto = location.protocol === "https:" ? "wss" : "ws";
      const wsUrl = `${proto}://${location.host}/ws/status`;
      console.log("[WS] connecting", wsUrl);

      ws = new WebSocket(wsUrl);

// --- WS watchdog + debug ---
try{
  const _t0 = Date.now();
  const _set = (s)=>{ try{ const el=document.getElementById("conn"); if(el) el.innerText = s; }catch(e){} };

  _set("WS: connecting…");

  // Se resta in CONNECTING troppo a lungo, forziamo close -> vediamo onclose + reconnect
  const _wd = setTimeout(() => {
    try{
      if(ws && ws.readyState === 0){
        console.warn("[WS] still CONNECTING after 4s -> forcing close()");
        _set("WS: stuck (restarting)…");
        ws.close();
      }
    }catch(e){}
  }, 4000);

  ws.addEventListener("open", () => {
    clearTimeout(_wd);
    console.log("[WS] open in", (Date.now()-_t0), "ms"); _set("WS: connected"); });

  ws.addEventListener("error", (ev) => {
    console.warn("[WS] error event", ev);
  });

  ws.addEventListener("close", (ev) => {
    console.warn("[WS] close", {code: ev.code, reason: ev.reason, wasClean: ev.wasClean, dt_ms:(Date.now()-_t0)});
  });
}catch(e){
  console.warn("[WS] watchdog setup failed", e);
}

      ws.onopen = () => {
        $("conn").innerText = "WS: connected";
        pushEvent("Connessione", "WebSocket status connesso.", "info");
      };

      ws.onerror = (e) => {
        console.warn("[WS] error", e);
        $("conn").innerText = "WS: error";
        pushEvent("Connessione", "Errore WebSocket (vedi console).", "warn");
      };

      ws.onclose = () => {
        $("conn").innerText = "WS: reconnecting…";
        setTimeout(connectWS, 1200);
      };

      let _wsTimer = null;
      ws.onmessage = (ev) => {
  try{
    const j = JSON.parse(ev.data);

    // --- accetta più formati ---
    let sites = null;
    if(j && Array.isArray(j.sites)) sites = j.sites;
    else if(j && j.data && Array.isArray(j.data.sites)) sites = j.data.sites;
    else if(j && Array.isArray(j.data)) sites = j.data;               // es: {type:"sites", data:[...]}
    else if(j && j.payload && Array.isArray(j.payload.sites)) sites = j.payload.sites;

    if(sites){
      sitesStatus = sites;
    }

    // persisti elenco siti visto via WS
    try{
      const known = (sitesStatus||[]).map(s=>({
        site_id:String(s.site_id ?? s.id ?? s.site ?? ""),
        online:(("ok" in s)?!!s.ok: ("online" in s)?!!s.online : true),
        last_edge_status:(s.last_ts||s.ts||null)
      })).filter(x=>x.site_id);
      localStorage.setItem("knownSites", JSON.stringify(known.slice(0,200)));
    }catch(e){}

    if(_wsTimer) return;
    _wsTimer = setTimeout(() => {
      _wsTimer = null;
      refreshSelectedKpi();
      if(selected && !selectedDevice) fetchSeriesPV();
      if(selected) loadWeatherForSelected();
      renderSites();
    }, 1200);
  }catch(e){}
};
    }catch(e){
      $("conn").innerText = "WS: init error";
      pushEvent("Connessione", "Impossibile inizializzare WebSocket.", "err");
    }
}
  // ----- Bindings -----
  $("q").addEventListener("input", renderSites);
  window.addEventListener("resize", () => { if(chartPV) chartPV.resize(); if(chartInv) chartInv.resize(); if(chartWeather) chartWeather.resize(); });

  $("btnRefresh").addEventListener("click", () => { fetchSites(); if(selected && !selectedDevice) fetchSeriesPV(); if(selected) loadWeatherForSelected(); });
  $("btnClearEvents").addEventListener("click", () => { $("events").innerHTML = ""; });

  $("mbAdd").addEventListener("click", mbAddEmpty);

  $("btnLoadAll").addEventListener("click", () => {
    const siteId = $("cfgSiteId").value.trim();
    if(!siteId) return;
    mbLoad(siteId);
    wxLoad(siteId);
  });

  $("btnSaveAll").addEventListener("click", () => {
    const siteId = $("cfgSiteId").value.trim();
    if(!siteId) return;
    mbSave(siteId);
    wxSave(siteId);
  });

  $("wxLoad").addEventListener("click", () => {
    const siteId = $("cfgSiteId").value.trim();
    if(!siteId) return;
    wxLoad(siteId);
  });

  $("wxSave").addEventListener("click", () => {
    const siteId = $("cfgSiteId").value.trim();
    if(!siteId) return;
    wxSave(siteId);
  });

  $("wxShow").addEventListener("click", () => {
    showWeatherPanel();
    closeDrawer();
  });

  // Boot
  $("hostLabel").innerText = location.host;
  try{ $("hostLabel").innerText = location.host || "—"; }catch(e){}; pushEvent("Boot", "Dashboard avviata. In attesa di telemetria.", "info");
  fetchSites();
  connectWS();
  showPVView();
});
/* ===========================
   Inverter data polling (latest + series)
   =========================== */
let __invPollTimer = null;
let __selectedInv = { siteId: null, deviceId: null };

async function refreshInverterData(siteId, deviceId){
  if(!siteId || !deviceId) return;
  try{
    // latest
    const r1 = await fetch(`/api/site/${encodeURIComponent(siteId)}/device/${encodeURIComponent(deviceId)}/latest`, { cache:"no-store" });
    const latest = await r1.json();

    // aggiorna card potenza / dettagli (se esistono gli elementi)
    const p = latest?.fields?.p_ac_w;
    if (typeof p === "number"){
      const el = document.getElementById("inv_p_ac_w");
      if(el) el.textContent = `${Math.round(p)} W`;
    }
    const ts = latest?.last_ts;
    const elts = document.getElementById("inv_last_ts");
    if(elts && ts) elts.textContent = ts;

    // series per chart (30 min)
    const r2 = await fetch(`/api/site/${encodeURIComponent(siteId)}/device/${encodeURIComponent(deviceId)}/series?minutes=30&every=10s`, { cache:"no-store" });
    const series = await r2.json();

    // se hai già una funzione che aggiorna il grafico, chiamala qui:
    // esempio: updateInvChart(series)
    if (typeof window.updateInvChart === "function") {
      window.updateInvChart(series);
    } else {
      // fallback: se usi echarts e hai un'istanza globale "invChart"
      if (window.invChart && series?.t?.length){
        const x = series.t;
        const y = series.p_ac_w || [];
        window.invChart.setOption({
          xAxis: { type: "category", data: x },
          yAxis: { type: "value" },
          series: [{ type: "line", data: y }]
        });
      }
    }

  } catch(e){
    console.warn("refreshInverterData error:", e);
  }
}

function selectInverter(siteId, deviceId){
  __selectedInv = { siteId, deviceId };

  // refresh immediato
  refreshInverterData(siteId, deviceId);

  // polling
  if(__invPollTimer) clearInterval(__invPollTimer);
  __invPollTimer = setInterval(() => refreshInverterData(__selectedInv.siteId, __selectedInv.deviceId), 5000);
}

// Hook: click su elementi con data-site-id e data-device-id (mettili dove renderizzi la lista inverter)
document.addEventListener("click", (ev) => {
  const t = ev.target?.closest?.("[data-site-id][data-device-id]");
  if(!t) return;
  const siteId = t.getAttribute("data-site-id");
  const deviceId = t.getAttribute("data-device-id");
  if(siteId && deviceId) selectInverter(siteId, deviceId);
});
