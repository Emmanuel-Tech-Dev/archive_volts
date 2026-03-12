const express = require("express");
const multer = require("multer");

const fileController = require("../controller/fileController");

const router = express.Router();
const upload = multer();

router.post("/upload", upload.single("file"), fileController.uploadFile);

router.get("/download/:id", fileController.downloadFile);

module.exports = router;
