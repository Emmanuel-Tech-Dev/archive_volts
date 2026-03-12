const shardService = require("../services/shardService");
const localDriver = require("../drivers/localDriver");

exports.uploadFile = async (req, res) => {
  const file = req.file.buffer;

  const { shardA, shardB } = shardService.splitBuffer(file);

  const keyA = Date.now() + "_A";
  const keyB = Date.now() + "_B";

  await localDriver.save(keyA, shardA);
  await localDriver.save(keyB, shardB);

  res.json({
    message: "File sharded",
    shards: [keyA, keyB],
  });
};

exports.downloadFile = async (req, res) => {
  res.json({
    message: "Download endpoint working",
  });
};
