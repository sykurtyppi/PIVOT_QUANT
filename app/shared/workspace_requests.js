(function attachWorkspaceRequestHelpers(global) {
  function beginWorkspaceRequest(state, workspaceKey, counterKey = 'requestSeq') {
    const workspace = state && state[workspaceKey];
    if (!workspace || typeof workspace !== 'object') return 0;
    const nextValue = Number(workspace[counterKey] || 0) + 1;
    workspace[counterKey] = nextValue;
    return nextValue;
  }

  function isWorkspaceRequestCurrent(state, workspaceKey, requestSeq, counterKey = 'requestSeq') {
    const workspace = state && state[workspaceKey];
    if (!workspace || typeof workspace !== 'object') return false;
    return Number(workspace[counterKey] || 0) === Number(requestSeq || 0);
  }

  global.PQWorkspaceRequests = Object.freeze({
    beginWorkspaceRequest,
    isWorkspaceRequestCurrent,
  });
})(window);
