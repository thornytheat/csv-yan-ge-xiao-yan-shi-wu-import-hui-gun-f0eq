// 原生 JS：无框架、无打包器、无 CDN。仅做「表头自检」「重复导入验证」「提交防抖」三件事。
(function () {
  function qs(sel, root) { return (root || document).querySelector(sel); }
  function qsa(sel, root) { return Array.prototype.slice.call((root || document).querySelectorAll(sel)); }

  function esc(text) {
    return String(text == null ? '' : text).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }

  window.postJSON = async function (url, payload) {
    var res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload || {})
    });
    var data = null;
    try { data = await res.json(); } catch (e) { data = { detail: '响应不是 JSON' }; }
    if (!res.ok) {
      var msg = (data && (data.detail || data.message)) || ('请求失败：' + res.status);
      throw new Error(msg);
    }
    return data;
  };

  window.showError = function (host, err) {
    if (!host) return;
    host.hidden = false;
    host.textContent = '执行失败：' + (err && err.message ? err.message : String(err));
  };

  // ---- 表头自检（模块 2 的独立入口）
  var contractBtn = qs('#contract-btn');
  var contractExtraBtn = qs('#contract-extra-btn');
  var contractOut = qs('#contract-result');
  var contractErr = qs('#contract-error');

  async function checkHeader(headerText) {
    if (!contractOut) return;
    if (contractErr) contractErr.hidden = true;
    contractOut.innerHTML = '<span class="muted">检查中…</span>';
    var policy = qs('#extra_policy') ? qs('#extra_policy').value : 'reject';
    try {
      var data = await window.postJSON('/api/contract-check', {
        header: headerText,
        extra_policy: policy
      });
      var rows = ['<p><b>' + (data.ok ? '契约通过' : '契约未通过') + '</b>'
        + '<span class="muted">（策略：' + esc(data.extra_policy) + '，列顺序无关）</span></p>'];

      if (!data.ok) {
        rows.push('<table><thead><tr><th>错误</th><th>说明</th></tr></thead><tbody>');
        (data.errors || []).forEach(function (e) {
          rows.push('<tr><td><code>' + esc(e.code) + '</code></td><td>' + esc(e.message) + '</td></tr>');
        });
        rows.push('</tbody></table>');
      }
      if ((data.warnings || []).length) {
        rows.push('<table><thead><tr><th>告警</th><th>说明</th></tr></thead><tbody>');
        data.warnings.forEach(function (e) {
          rows.push('<tr><td><code>' + esc(e.code) + '</code></td><td>' + esc(e.message) + '</td></tr>');
        });
        rows.push('</tbody></table>');
      }
      rows.push('<p class="muted">解析到的列：' + esc((data.header || []).join(' | ')) + '</p>');
      contractOut.innerHTML = rows.join('');
    } catch (err) {
      contractOut.innerHTML = '';
      window.showError(contractErr, err);
    }
  }

  if (contractBtn) {
    contractBtn.addEventListener('click', function () {
      var input = qs('#header-input');
      checkHeader(input ? input.value : '');
    });
  }
  if (contractExtraBtn) {
    contractExtraBtn.addEventListener('click', function () {
      var input = qs('#header-input');
      if (input) input.value = 'site,category,order_id,order_date,amount,qty,note';
      checkHeader('site,category,order_id,order_date,amount,qty,note');
    });
  }

  // ---- 重复导入验证（幂等复核）
  qsa('[data-replay-sha]').forEach(function (btn) {
    btn.addEventListener('click', async function () {
      var host = qs('#replay-result');
      var errHost = qs('#replay-error');
      if (errHost) errHost.hidden = true;
      btn.disabled = true;
      try {
        var data = await window.postJSON('/api/replay-check', { sha256: btn.getAttribute('data-replay-sha') });
        if (host) {
          host.hidden = false;
          host.innerHTML = '<b>' + esc(btn.getAttribute('data-batch')) + '</b> → '
            + '<span class="tag ' + (data.verdict === 'duplicate_file' ? 'warn' : 'ok') + '">'
            + esc(data.verdict) + '</span> ' + esc(data.message)
            + ' <span class="muted">当前明细 ' + esc(data.fact_rows) + ' 行</span>';
        }
      } catch (err) {
        window.showError(errHost, err);
      } finally {
        btn.disabled = false;
      }
    });
  });

  // ---- 上传表单：提交后禁用按钮并提示，避免重复点击触发两次上传
  var form = qs('#upload-form');
  if (form) {
    form.addEventListener('submit', function () {
      var btn = qs('#submit-btn');
      var hint = qs('#upload-hint');
      if (btn) { btn.disabled = true; btn.textContent = '校验中…'; }
      if (hint) hint.textContent = '正在探测编码、校验契约与逐行规则，请稍候。';
    });
  }

  // ---- 结果页：分段标签（聚合 / 探测证据 / 明细）
  qsa('[data-tab-target]').forEach(function (btn) {
    btn.addEventListener('click', function () {
      var group = btn.closest('.segmented, .tabs');
      if (group) {
        qsa('button', group).forEach(function (b) { b.classList.remove('active'); });
      }
      btn.classList.add('active');
      var host = btn.closest('.panel') || document;
      qsa('.tab-panel', host).forEach(function (p) {
        p.hidden = p.id !== btn.getAttribute('data-tab-target');
      });
    });
  });

  // ---- 复核页：搜索/筛选 + 选中批次在右侧看回执详情
  var reviewSearch = qs('#review-search');
  var reviewScope = qs('#review-scope');
  var reviewEncoding = qs('#review-encoding');
  var batchesTable = qs('#batches-table');
  var attemptsTable = qs('#attempts-table');
  var reviewStat = qs('#review-stat');

  function applyReviewFilters() {
    if (!batchesTable) return;
    var term = (reviewSearch ? reviewSearch.value : '').trim().toLowerCase();
    var scope = reviewScope ? reviewScope.value : '';
    var enc = reviewEncoding ? reviewEncoding.value : '';
    var shown = 0;
    var total = 0;
    qsa('tr[data-batch]', batchesTable).forEach(function (row) {
      total += 1;
      var text = (row.getAttribute('data-search') || '').toLowerCase();
      var ok = (!term || text.indexOf(term) >= 0)
        && (!enc || (row.getAttribute('data-encoding') || '').indexOf(enc) === 0)
        && (!scope || row.getAttribute('data-' + scope) === '1');
      row.hidden = !ok;
      if (ok) shown += 1;
    });
    if (attemptsTable) {
      qsa('tr[data-search]', attemptsTable).forEach(function (row) {
        var text = (row.getAttribute('data-search') || '').toLowerCase();
        row.hidden = !!term && text.indexOf(term) < 0;
      });
    }
    if (reviewStat) reviewStat.textContent = '共 ' + shown + ' / ' + total + ' 条批次';
  }

  [reviewSearch, reviewScope, reviewEncoding].forEach(function (el) {
    if (!el) return;
    el.addEventListener('input', applyReviewFilters);
    el.addEventListener('change', applyReviewFilters);
  });

  function showDetail(row) {
    var fields = { batch: '#detail-batch', filename: '#detail-filename', encoding: '#detail-encoding',
      rows: '#detail-rows', amount: '#detail-amount', time: '#detail-time', sha: '#detail-sha' };
    Object.keys(fields).forEach(function (key) {
      var el = qs(fields[key]);
      var value = row.getAttribute('data-' + key);
      if (el && value != null) el.textContent = value;
    });
    var link = qs('#detail-link');
    if (link) link.setAttribute('href', row.getAttribute('data-href') || '/workbench');
    var tag = qs('#detail-tag');
    if (tag) {
      tag.textContent = row.getAttribute('data-forced') === '1' ? 'forced 重灌'
        : (row.getAttribute('data-superseded') === '1' ? '已被替代' : '已入库');
    }
  }

  if (batchesTable) {
    qsa('tr[data-batch]', batchesTable).forEach(function (row) {
      row.addEventListener('click', function (ev) {
        if (ev.target.closest('a, button')) return;
        qsa('tr[data-batch]', batchesTable).forEach(function (r) { r.classList.remove('selected'); });
        row.classList.add('selected');
        showDetail(row);
      });
    });
  }

  // ---- 模块 7/8：分片续传、整批隔离与原子切换
  var chunkFile = qs('#chunk-file');
  var chunkVersion = qs('#chunk-version');
  var chunkSize = qs('#chunk-size');
  var chunkAmounts = qs('#chunk-amounts');
  var chunkBtn = qs('#chunk-upload-btn');
  var finalizeBtn = qs('#chunk-finalize-btn');
  var statusBtn = qs('#chunk-status-btn');
  var chunkProgress = qs('#chunk-progress');
  var chunkResult = qs('#chunk-result');
  var chunkError = qs('#chunk-error');

  function sha256Hex(buf) {
    return crypto.subtle.digest('SHA-256', buf).then(function (digest) {
      return Array.prototype.map.call(new Uint8Array(digest), function (b) {
        return ('00' + b.toString(16)).slice(-2);
      }).join('');
    });
  }

  function parseAmountsFromCsv(text) {
    var lines = text.split(/\r?\n/).filter(function (l) { return l.trim().length; });
    if (lines.length < 2) return [];
    var header = lines[0].split(',').map(function (c) { return c.trim(); });
    var idx = {};
    header.forEach(function (h, i) { idx[h] = i; });
    var buckets = {};
    var order = [];
    for (var i = 1; i < lines.length; i++) {
      var cells = lines[i].split(',');
      var site = (cells[idx.site] || '').trim();
      if (!site) continue;
      if (!buckets[site]) { buckets[site] = { site: site, amount_cents: 0, qty: 0, row_count: 0 }; order.push(site); }
      buckets[site].amount_cents += Math.round(parseFloat(cells[idx.amount]) * 100);
      buckets[site].qty += parseInt(cells[idx.qty], 10);
      buckets[site].row_count += 1;
    }
    return order.map(function (s) { return buckets[s]; });
  }

  function showChunkError(err) {
    if (!chunkError) return;
    chunkError.hidden = false;
    chunkError.textContent = '分片流程失败：' + (err && err.message ? err.message : String(err));
  }

  function renderSession(data) {
    if (!chunkResult) return;
    var statusTag = {
      open: 'ok', quarantined: 'err', published: 'ok', stale_rejected: 'err'
    }[data.status] || '';
    var html = '<p>会话 <code>' + esc((data.file_id || '').slice(0, 16)) + '…</code>'
      + ' 状态 <span class="tag ' + statusTag + '">' + esc(data.status) + '</span>'
      + ' 已收 ' + esc(data.received_count) + '/' + esc(data.total_segments) + ' 段</p>';
    if (data.gaps && data.gaps.length) {
      html += '<p class="muted">缺口段号：' + esc(data.gaps.join(', ')) + '</p>';
    }
    if (data.quarantine_reason) {
      html += '<p style="color:var(--red,#b00020)">隔离原因：' + esc(data.quarantine_reason) + '</p>';
    }
    if (data.mismatch && data.mismatch.length) {
      html += '<table><thead><tr><th>错配码</th><th>依据</th></tr></thead><tbody>';
      data.mismatch.slice(0, 10).forEach(function (m) {
        html += '<tr><td><code>' + esc(m.code) + '</code></td><td>' + esc(m.message) + '</td></tr>';
      });
      html += '</tbody></table>';
    }
    if (data.batch_id) html += '<p>发布批次：<code>' + esc(data.batch_id) + '</code></p>';
    chunkResult.innerHTML = html;
  }

  async function buildManifest(file) {
    var buf = await file.arrayBuffer();
    var bytes = new Uint8Array(buf);
    var sha = await sha256Hex(buf);
    var segSize = Math.max(256, parseInt(chunkSize.value, 10) || 65536);
    var totalSegments = Math.ceil(file.size / segSize);
    var text = new TextDecoder('utf-8').decode(bytes);
    var amounts;
    if (chunkAmounts && chunkAmounts.value.trim()) {
      amounts = JSON.parse(chunkAmounts.value);
    } else {
      amounts = parseAmountsFromCsv(text);
    }
    var totalRows = amounts.reduce(function (n, a) { return n + a.row_count; }, 0);
    var manifest = {
      filename: file.name,
      version: (chunkVersion && chunkVersion.value.trim()) || 'v1',
      sha256: sha,
      total_size: file.size,
      segment_size: segSize,
      total_segments: totalSegments,
      total_rows: totalRows,
      amounts: amounts
    };
    return { manifest: manifest, bytes: bytes, segSize: segSize };
  }

  function postForm(url, form) {
    return fetch(url, { method: 'POST', body: form }).then(function (res) {
      return res.json().then(function (data) { return { ok: res.ok, status: res.status, data: data }; });
    });
  }

  if (chunkBtn) {
    chunkBtn.addEventListener('click', async function () {
      if (!chunkFile || !chunkFile.files.length) { showChunkError(new Error('请先选择 CSV 文件')); return; }
      chunkError.hidden = true;
      chunkBtn.disabled = true;
      try {
        var built = await buildManifest(chunkFile.files[0]);
        var manifest = built.manifest;
        var bytes = built.bytes;
        var segSize = built.segSize;

        var openForm = new FormData();
        openForm.append('manifest', JSON.stringify(manifest));
        var opened = await postForm('/api/chunks/session', openForm);
        if (!opened.ok) throw new Error((opened.data.detail && opened.data.detail.message) || '打开会话失败');

        for (var i = 0; i < manifest.total_segments; i++) {
          var part = bytes.slice(i * segSize, Math.min((i + 1) * segSize, bytes.length));
          var partSha = await sha256Hex(part);
          var form = new FormData();
          form.append('manifest', JSON.stringify(manifest));
          form.append('index', String(i));
          form.append('checksum', partSha);
          form.append('file', new Blob([part]), i + '.part');
          var up = await fetch('/api/chunks/segment', { method: 'PUT', body: form })
            .then(function (res) { return res.json().then(function (d) { return { ok: res.ok, data: d }; }); });
          if (!up.ok) {
            renderSession(up.data.session || {});
            throw new Error(up.data.detail || ('第 ' + i + ' 段被拒收（已整批隔离）'));
          }
          if (chunkProgress) {
            chunkProgress.textContent = '已上传 ' + (i + 1) + '/' + manifest.total_segments + ' 段';
          }
        }
        renderSession((await postForm('/api/chunks/session', openForm)).data);
        if (chunkProgress) chunkProgress.textContent = '全部片段已收齐，可点「收齐后核对并切换」。';
      } catch (err) {
        showChunkError(err);
      } finally {
        chunkBtn.disabled = false;
      }
    });
  }

  if (finalizeBtn) {
    finalizeBtn.addEventListener('click', async function () {
      if (!chunkFile || !chunkFile.files.length) { showChunkError(new Error('请先选择 CSV 文件')); return; }
      chunkError.hidden = true;
      try {
        var built = await buildManifest(chunkFile.files[0]);
        var form = new FormData();
        form.append('manifest', JSON.stringify(built.manifest));
        var res = await fetch('/api/chunks/finalize', { method: 'POST', body: form });
        var data = await res.json();
        if (!res.ok) {
          if (data.status === 'quarantined') renderSession(data);
          throw new Error(data.detail || data.reason || ('finalize 返回 ' + res.status));
        }
        chunkProgress.textContent = '已一次性切换当前版本（批次 ' + data.batch_id + '）。';
        var statusForm = new FormData();
        statusForm.append('manifest', JSON.stringify(built.manifest));
        renderSession((await postForm('/api/chunks/session', statusForm)).data);
      } catch (err) {
        showChunkError(err);
      }
    });
  }

  if (statusBtn) {
    statusBtn.addEventListener('click', async function () {
      if (!chunkFile || !chunkFile.files.length) { showChunkError(new Error('请先选择 CSV 文件')); return; }
      try {
        var built = await buildManifest(chunkFile.files[0]);
        var res = await fetch('/api/chunks/session/' + built.manifest.sha256);
        var data = await res.json();
        if (!res.ok) throw new Error(data.detail || '会话不存在');
        renderSession(data);
      } catch (err) {
        showChunkError(err);
      }
    });
  }
})();
