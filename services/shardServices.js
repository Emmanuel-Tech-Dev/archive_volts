function splitBuffer(buffer) {
  const shardA = [];
  const shardB = [];

  for (let i = 0; i < buffer.length; i++) {
    if (i % 2 === 0) {
      shardA.push(buffer[i]);
    } else {
      shardB.push(buffer[i]);
    }
  }

  return {
    shardA: Buffer.from(shardA),
    shardB: Buffer.from(shardB),
  };
}

module.exports = { splitBuffer };
