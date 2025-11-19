# Bridge Patches for Debezium Server

This document explains how to apply Bridge-specific customizations to new Debezium Server versions using git patches.

## Overview

We maintain Bridge-specific customizations as git patches that can be applied to any Debezium Server version. This approach provides:

- ✅ **Version independence** - Apply patches to any upstream version
- ✅ **Maintainable updates** - No manual file editing required  
- ✅ **Git history preservation** - Proper commit tracking and authorship
- ✅ **Automated conflict resolution** - Using git's 3-way merge

## Available Patches

### `fast-jar-image-build.patch`
**Main production patch** containing:

- **Multi-stage Dockerfile** with OpenTelemetry and Jolokia monitoring
- **Advanced bridge-run.sh** with smart JAR detection (fast-jar vs classpath)
- **GitHub Actions workflow** for automated image building
- **Fast-jar packaging** configuration in pom.xml

**Features:**
- OpenTelemetry tracing agent
- Jolokia JMX monitoring  
- Smart Quarkus fast-jar detection
- CI/CD pipeline for ECR deployment

## Applying Patches to New Versions

### Method 1: Clean Application (Recommended)

```bash
# Start from a clean upstream tag
git checkout v3.4.0.Final  # or desired version
git checkout -b custom-build-v3.4.0.Final

# Apply patch with 3-way merge (handles conflicts automatically)
git am --3way bridge-patches/fast-jar-image-build.patch
```

### Method 2: If 3-way merge fails

If the patch can't be applied automatically:

```bash
# See what failed
git am --show-current-patch=diff

# Resolve manually and continue
git add .
git am --continue

# Or abort and try different approach
git am --abort
```

## Troubleshooting

### "error: patch failed: pom.xml:39"

**Solution:** Use 3-way merge which automatically resolves pom.xml structure differences:
```bash
git am --3way bridge-patches/fast-jar-image-build.patch
```

### "error: Dockerfile: already exists in index"

**Solution:** Remove conflicting files first:
```bash
git rm Dockerfile bridge-run.sh  # if they exist
git commit -m "Remove old files"
git am --3way bridge-patches/fast-jar-image-build.patch
```

### "fatal: previous rebase directory .git/rebase-apply still exists"

**Solution:** Clean up previous failed attempts:
```bash
git am --abort
git am --3way bridge-patches/fast-jar-image-build.patch
```

## Creating New Patches

If you need to create a new patch from your customizations:

```bash
# After making your changes
git add .
git commit -m "Your changes"

# Create patch from the last N commits
git format-patch -1 HEAD --stdout > bridge-patches/your-patch-name.patch
```

## Patch Contents Summary

### Dockerfile
- Multi-stage build (downloader → builder → final)
- OpenJDK 21 base images
- OpenTelemetry agent download
- Jolokia JMX agent integration
- Proper permissions and security

### bridge-run.sh  
- Smart JAR detection (quarkus-run.jar vs classpath mode)
- OpenTelemetry and Jolokia agent loading
- JMX remote monitoring configuration
- Pulsar service account handling

### GitHub Workflow
- Automated builds on push
- Core Debezium building
- Server building with fast-jar
- ECR deployment pipeline

### POM Configuration
- Fast-jar packaging type for optimized startup
- Quarkus Maven plugin configuration

## Version Compatibility

This patch has been tested with:
- ✅ Debezium Server v3.3.0.Final

For older versions, manual adaptation may be required.

## Building Production Images

After applying patches:

```bash
# Build the server
./mvnw clean package -DskipITs -DskipTests -Passembly

# Build Docker image  
docker build -t debezium-server-bridge .

# Or use the automated CI/CD pipeline via GitHub Actions
```

---

**Note:** Always test patch application on a branch before applying to main development lines.